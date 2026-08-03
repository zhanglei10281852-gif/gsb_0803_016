import assert from "node:assert/strict";
import { fork, spawn, execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, test } from "node:test";
import Database from "better-sqlite3";
import { createRecognitionStore, RecognitionStore } from "../src";
import type { RecognitionEventInput, Snapshot } from "../src";
import {
  E2E_SESSION_ID,
  expectedE2EFinalText,
  makeE2EEvents,
  shuffleE2EEvents,
} from "./e2e-scenarios";

const stores: RecognitionStore[] = [];
const paths: string[] = [];
const forkedChildren: Array<ReturnType<typeof fork>> = [];
const children: Array<ReturnType<typeof spawn>> = [];

function createStore(filename = join(tempPath(), "e2e.db")): {
  store: RecognitionStore;
  filename: string;
} {
  const store = createRecognitionStore({ filename, busyTimeout: 15000 });
  stores.push(store);
  return { store, filename };
}

function tempPath(): string {
  const dir = mkdtempSync(join(tmpdir(), "asr-revision-e2e-"));
  paths.push(dir);
  return dir;
}

function openRawDatabase(filename: string): Database.Database {
  const db = new Database(filename, { timeout: 15000 });
  db.pragma("busy_timeout = 15000");
  return db;
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function blockingWait(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function removePath(path: string): void {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    try {
      rmSync(path, { recursive: true, force: true });
      return;
    } catch (error) {
      if (
        attempt === 19 ||
        !(error instanceof Error) ||
        !("code" in error) ||
        (error.code !== "EBUSY" &&
          error.code !== "EPERM" &&
          error.code !== "ENOENT")
      ) {
        throw error;
      }
      blockingWait(100);
    }
  }
}

async function killProcess(child: ReturnType<typeof spawn>): Promise<void> {
  if (child.exitCode === null && child.signalCode === null) {
    if (process.platform === "win32" && child.pid) {
      try {
        execFileSync("taskkill", ["/pid", String(child.pid), "/f", "/t"], {
          stdio: "ignore",
        });
      } catch {
        child.kill("SIGKILL");
      }
    } else {
      child.kill("SIGKILL");
    }
  }

  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  await new Promise<void>((resolve) => child.once("exit", () => resolve()));
}

async function runIngestWorker(
  filename: string,
  events: RecognitionEventInput[],
  seed: number,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = fork(join(__dirname, "ingest-child.ts"), [], {
      execArgv: ["--import", "tsx"],
      stdio: "ignore",
    });
    forkedChildren.push(child);

    let settled = false;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      child.off("message", onMessage);
      child.off("error", onError);
      child.off("exit", onExit);
      if (error) reject(error);
      else resolve();
    };
    const onMessage = (message: { ok?: boolean; message?: string }) => {
      if (message.ok) finish();
      else finish(new Error(message.message ?? "ingest child failed"));
    };
    const onError = (error: Error) => finish(error);
    const onExit = (code: number | null) => {
      if (!settled && code !== 0) {
        finish(new Error(`ingest child exited with code ${code ?? "null"}`));
      }
    };
    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
      finish(new Error("ingest child timed out"));
    }, 30000);

    child.on("message", onMessage);
    child.on("error", onError);
    child.once("exit", onExit);
    child.send({ filename, events, seed });
  });
}

interface CompactChildRequest {
  filename: string;
  sessionId: string;
  rounds: number;
  writer?: {
    sourceId: string;
    count: number;
    delayMs: number;
    seed: number;
  };
}

function runCompactWorker(request: CompactChildRequest): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = fork(join(__dirname, "compact-child.ts"), [], {
      execArgv: ["--import", "tsx"],
      stdio: "ignore",
    });
    forkedChildren.push(child);

    let settled = false;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      child.off("message", onMessage);
      child.off("error", onError);
      child.off("exit", onExit);
      if (error) reject(error);
      else resolve();
    };
    const onMessage = (message: { ok?: boolean; message?: string }) => {
      if (message.ok) finish();
      else finish(new Error(message.message ?? "compact child failed"));
    };
    const onError = (error: Error) => finish(error);
    const onExit = (code: number | null) => {
      if (!settled && code !== 0) {
        finish(new Error(`compact child exited with code ${code ?? "null"}`));
      }
    };
    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
      finish(new Error("compact child timed out"));
    }, 30000);

    child.on("message", onMessage);
    child.on("error", onError);
    child.once("exit", onExit);
    child.send(request);
  });
}

function comparableSnapshot(snapshot: Snapshot) {
  return {
    segments: snapshot.segments,
    text: snapshot.text,
    finalText: snapshot.finalText,
    summary: {
      eventCount: snapshot.summary.eventCount,
      sourceCount: snapshot.summary.sourceCount,
      finalSegmentCount: snapshot.summary.finalSegmentCount,
      activePartialCount: snapshot.summary.activePartialCount,
      textLength: snapshot.summary.textLength,
      finalTextLength: snapshot.summary.finalTextLength,
    },
  };
}

function buildExpectedSnapshot(): Snapshot {
  const { store } = createStore(":memory:");
  const events = makeE2EEvents().sort((a, b) => {
    if (a.sourceId < b.sourceId) return -1;
    if (a.sourceId > b.sourceId) return 1;
    if (a.sourceSeq !== b.sourceSeq) return a.sourceSeq - b.sourceSeq;
    if (a.isFinal !== b.isFinal) return a.isFinal ? -1 : 1;
    return a.eventId < b.eventId ? -1 : 1;
  });
  store.ingest(E2E_SESSION_ID, events);
  return store.getSnapshot(E2E_SESSION_ID);
}

function assertRevisionIntegrity(filename: string): void {
  const db = openRawDatabase(filename);
  try {
    const counts = db
      .prepare(
        `SELECT
           COUNT(*) AS event_count,
           (SELECT COUNT(*) FROM revisions WHERE session_id = events.session_id) AS revision_count,
           (SELECT MAX(revision) FROM revisions WHERE session_id = events.session_id) AS max_revision
         FROM events
         WHERE session_id = ?`,
      )
      .get(E2E_SESSION_ID) as
      | { event_count: number; revision_count: number; max_revision: number }
      | undefined;

    assert.ok(counts, "session should contain committed events");
    assert.equal(counts.revision_count, counts.max_revision);

    const revisions = db
      .prepare(
        `SELECT revision, snapshot
         FROM revisions
         WHERE session_id = ?
         ORDER BY revision ASC`,
      )
      .all(E2E_SESSION_ID) as Array<{ revision: number; snapshot: string }>;

    let previous = 0;
    for (const row of revisions) {
      assert.equal(row.revision, previous + 1);
      previous = row.revision;
      const snapshot = JSON.parse(row.snapshot) as Snapshot;
      assert.equal(snapshot.revision, row.revision);
    }
  } finally {
    db.close();
  }
}

afterEach(async () => {
  await Promise.allSettled(
    forkedChildren.splice(0).map((child) => killProcess(child)),
  );
  await Promise.allSettled(
    children.splice(0).map((child) => killProcess(child)),
  );
  while (stores.length > 0) {
    stores.pop()?.close();
  }
  blockingWait(250);
  for (const path of paths.splice(0)) {
    try {
      removePath(path);
    } catch (error) {
      if (
        !(error instanceof Error) ||
        !("code" in error) ||
        (error.code !== "EBUSY" && error.code !== "EPERM")
      ) {
        throw error;
      }
    }
  }
});

test("multiple worker instances concurrently converge to one deterministic snapshot", async () => {
  const dir = tempPath();
  const filename = join(dir, "concurrent.db");
  const allEvents = makeE2EEvents();
  const workerEvents = [
    shuffleE2EEvents(allEvents, 11),
    shuffleE2EEvents(allEvents, 23),
    shuffleE2EEvents(allEvents, 37),
    shuffleE2EEvents(allEvents, 53),
  ];

  await Promise.all(
    workerEvents.map((events, index) =>
      runIngestWorker(filename, events, (index + 1) * 17),
    ),
  );

  const { store } = createStore(filename);
  const actual = store.getSnapshot(E2E_SESSION_ID);
  const expected = buildExpectedSnapshot();

  assert.equal(actual.finalText, expectedE2EFinalText());
  assert.deepEqual(comparableSnapshot(actual), comparableSnapshot(expected));
  assertRevisionIntegrity(filename);
});

test("killing a process mid-ingest leaves no half transaction and resume converges", async () => {
  const dir = tempPath();
  const filename = join(dir, "crash.db");
  const child = spawn(
    process.execPath,
    ["--import", "tsx", join(__dirname, "crash-child.ts")],
    {
      env: { ...process.env, E2E_DB: filename },
      stdio: "ignore",
    },
  );
  children.push(child);

  const db = openRawDatabase(filename);
  try {
    let maxRevision = 0;
    for (let attempt = 0; attempt < 500 && maxRevision === 0; attempt += 1) {
      await wait(5);
      const table = db
        .prepare(
          `SELECT 1 AS exists_flag
           FROM sqlite_master
           WHERE type = 'table' AND name = 'revisions'`,
        )
        .get() as { exists_flag: number } | undefined;
      if (!table) continue;

      const row = db
        .prepare(
          `SELECT COALESCE(MAX(revision), 0) AS revision
           FROM revisions WHERE session_id = ?`,
        )
        .get(E2E_SESSION_ID) as { revision: number };
      maxRevision = row.revision;
    }
    assert.ok(
      maxRevision > 0,
      "child should have committed at least one revision",
    );
  } finally {
    db.close();
  }

  await wait(25);
  await killProcess(child);
  blockingWait(50);

  assertRevisionIntegrity(filename);

  const { store } = createStore(filename);
  const existing = new Set(
    (
      openRawDatabase(filename)
        .prepare(`SELECT event_id FROM events WHERE session_id = ?`)
        .all(E2E_SESSION_ID) as Array<{ event_id: string }>
    ).map((row) => row.event_id),
  );
  const missing = shuffleE2EEvents(
    makeE2EEvents().filter((event) => !existing.has(event.eventId)),
    91,
  );

  for (const event of missing) {
    store.ingest(E2E_SESSION_ID, event);
    if (event.eventId.endsWith("0")) {
      blockingWait(1);
    }
  }

  const actual = store.getSnapshot(E2E_SESSION_ID);
  const expected = buildExpectedSnapshot();
  assert.equal(actual.finalText, expectedE2EFinalText());
  assert.deepEqual(comparableSnapshot(actual), comparableSnapshot(expected));
  assertRevisionIntegrity(filename);
});

test("slow consumer with small pages receives every revision without gaps or replay", async () => {
  const { store } = createStore();
  const consumer = store.createConsumer(E2E_SESSION_ID, "slow-qc");
  const allEvents = makeE2EEvents();
  const received: number[] = [];
  let nextExpected = 1;

  for (let i = 0; i < allEvents.length; i += 12) {
    const chunk = shuffleE2EEvents(allEvents.slice(i, i + 12), 101 + i);
    for (const event of chunk) {
      store.ingest(E2E_SESSION_ID, event);
    }

    let batch = consumer.fetch(1);
    while (batch.length > 0) {
      for (const revision of batch) {
        assert.equal(revision.revision, nextExpected);
        received.push(revision.revision);
        nextExpected += 1;
        blockingWait(1);
      }
      consumer.acknowledge(received[received.length - 1] as number);
      batch = consumer.fetch(1);
    }
  }

  let tail = consumer.fetch(1);
  while (tail.length > 0) {
    for (const revision of tail) {
      assert.equal(revision.revision, nextExpected);
      received.push(revision.revision);
      nextExpected += 1;
    }
    consumer.acknowledge(received[received.length - 1] as number);
    tail = consumer.fetch(1);
  }

  const finalRevision = store.getSnapshot(E2E_SESSION_ID).revision;
  assert.equal(received.length, finalRevision);
  assert.deepEqual(
    received,
    Array.from({ length: finalRevision }, (_, index) => index + 1),
  );
  assert.deepEqual(consumer.fetch(10), []);
  assert.equal(consumer.getCursor(), finalRevision);
});

test("concurrent writers and compaction preserve contiguous revisions and snapshot", async () => {
  const dir = tempPath();
  const filename = join(dir, "compact-concurrent.db");
  const sessionId = "concurrent-compact";
  const sources = ["writer-a", "writer-b", "writer-c"];
  const eventsPerWriter = 25;

  const writers = sources.map((sourceId, index) =>
    runCompactWorker({
      filename,
      sessionId,
      rounds: 0,
      writer: {
        sourceId,
        count: eventsPerWriter,
        delayMs: 4,
        seed: index + 11,
      },
    }),
  );
  const compactor = runCompactWorker({
    filename,
    sessionId,
    rounds: 60,
  });

  await Promise.all([...writers, compactor]);

  const { store } = createStore(filename);
  const snapshot = store.getSnapshot(sessionId);
  assert.equal(snapshot.segments.length, sources.length * eventsPerWriter);
  assert.equal(snapshot.summary.eventCount, sources.length * eventsPerWriter);

  const expectedFinalText = sources
    .flatMap((sourceId) =>
      Array.from(
        { length: eventsPerWriter },
        (_, seq) => `${sourceId}:${seq} `,
      ),
    )
    .join("");
  assert.equal(snapshot.finalText, expectedFinalText);

  const db = openRawDatabase(filename);
  try {
    const state = db
      .prepare(
        `SELECT baseline_revision, compacted_through_revision,
                total_revisions_removed
         FROM compaction_state WHERE session_id = ?`,
      )
      .get(sessionId) as
      | {
          baseline_revision: number;
          compacted_through_revision: number;
          total_revisions_removed: number;
        }
      | undefined;
    assert.ok(state, "compaction should have run");
    assert.ok(state!.total_revisions_removed > 0);
    assert.ok(state!.baseline_revision > 0);

    const revisions = db
      .prepare(
        `SELECT revision FROM revisions
         WHERE session_id = ?
         ORDER BY revision ASC`,
      )
      .all(sessionId) as Array<{ revision: number }>;

    let expected = state!.baseline_revision + 1;
    for (const row of revisions) {
      assert.equal(row.revision, expected);
      expected += 1;
    }

    const maxRevision =
      revisions.length > 0
        ? revisions.at(-1)!.revision
        : state!.baseline_revision;
    assert.equal(maxRevision, snapshot.revision);
    assert.equal(snapshot.revision, sources.length * eventsPerWriter);
  } finally {
    db.close();
  }
});
