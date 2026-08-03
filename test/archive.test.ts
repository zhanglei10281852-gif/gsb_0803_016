import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Readable } from "node:stream";
import { afterEach, test } from "node:test";
import {
  ArchiveImportConflictError,
  ArchiveIntegrityError,
  createRecognitionStore,
  RecognitionStore,
} from "../src";
import type { RecognitionEventInput } from "../src";

const stores: RecognitionStore[] = [];
const paths: string[] = [];

function createStore(filename?: string): {
  store: RecognitionStore;
  filename: string;
} {
  const resolved = filename ?? join(tempPath(), "archive.db");
  const store = createRecognitionStore({
    filename: resolved,
    busyTimeout: 5000,
  });
  stores.push(store);
  return { store, filename: resolved };
}

function tempPath(): string {
  const dir = mkdtempSync(join(tmpdir(), "asr-revision-archive-"));
  paths.push(dir);
  return dir;
}

afterEach(() => {
  while (stores.length > 0) {
    stores.pop()?.close();
  }
  for (const path of paths.splice(0)) {
    rmSync(path, { recursive: true, force: true });
  }
});

async function collectArchive(
  stream: AsyncIterable<string>
): Promise<string> {
  let out = "";
  for await (const chunk of stream) out += chunk;
  return out;
}

function seededSession(): RecognitionStore {
  const { store } = createStore();
  const events: RecognitionEventInput[] = [
    {
      eventId: "e1",
      sourceId: "a",
      sourceSeq: 0,
      isFinal: true,
      text: "hello ",
    },
    {
      eventId: "e2",
      sourceId: "a",
      sourceSeq: 1,
      isFinal: true,
      text: "world",
    },
    {
      eventId: "e3",
      sourceId: "b",
      sourceSeq: 0,
      isFinal: true,
      text: "!",
    },
  ];
  store.ingest("s", events);
  const base = store.getSnapshot("s").revision;
  const claim = store.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "alice",
    baseRevision: base,
    ttlMs: 60000,
  });
  store.submitCorrection("s", {
    leaseId: claim.lease.leaseId,
    actor: "alice",
    reason: "capitalization",
    correctedText: "Hello ",
    baseRevision: base,
  });
  store.acknowledge("s", "qc", base + 1);
  return store;
}

test("archive round-trips snapshot, revisions, corrections, leases, and cursors", async () => {
  const source = seededSession();
  const archive = await collectArchive(source.exportArchive("s"));

  const { store: target } = createStore();
  await target.importArchive("s", Readable.from(archive));

  assert.deepEqual(target.getSnapshot("s"), source.getSnapshot("s"));

  const sourceRevs = source.getSnapshot("s").revision;
  for (let r = 1; r <= sourceRevs; r += 1) {
    assert.deepEqual(
      target.getRevision("s", r),
      source.getRevision("s", r),
    );
  }

  const lineage = target.getCorrectionLineage("s", "a", 0);
  assert.equal(lineage.length, 1);
  assert.equal(lineage[0]!.actor, "alice");
  assert.equal(lineage[0]!.reason, "capitalization");
  assert.equal(lineage[0]!.baseRevision, 3);
  assert.equal(lineage[0]!.supersedes, undefined);

  const lease = target.getActiveLease("s", "a", 0);
  assert.equal(lease, undefined, "consumed lease should be released after import");
  const historicalLease = target.getLease(
    "s",
    source.getLease(
      "s",
      source.getCorrectionLineage("s", "a", 0)[0]!.leaseId,
    )!.leaseId,
  );
  assert.ok(historicalLease);
  assert.equal(historicalLease!.actor, "alice");

  assert.equal(target.getCursor("s", "qc"), source.getCursor("s", "qc"));

  const consumer = target.createConsumer("s", "qc");
  assert.deepEqual(consumer.fetch(), []);
});

test("re-importing identical archive is idempotent", async () => {
  const source = seededSession();
  const archive = await collectArchive(source.exportArchive("s"));

  const { store } = createStore();
  const first = await store.importArchive("s", Readable.from(archive));
  assert.ok(first.imported.events > 0);
  assert.ok(first.imported.revisions > 0);

  const second = await store.importArchive("s", Readable.from(archive));
  assert.equal(second.imported.events, 0);
  assert.equal(second.imported.revisions, 0);
  assert.equal(second.duplicates.events, first.imported.events + first.duplicates.events);
  assert.equal(second.duplicates.revisions, first.imported.revisions + first.duplicates.revisions);
  assert.equal(second.duplicates.corrections, 1);
  assert.equal(second.duplicates.cursors, 1);

  assert.deepEqual(store.getSnapshot("s"), source.getSnapshot("s"));
});

test("tampered archive fails with hash-mismatch before any state is visible", async () => {
  const source = seededSession();
  const archive = await collectArchive(source.exportArchive("s"));
  const lines = archive.split("\n").filter((l) => l.length > 0);
  const eventIndex = lines.findIndex((l) => l.includes('"eventId":"e1"'));
  assert.ok(eventIndex > 0, "expected to find e1 event line");
  const mutated = lines
    .map((line, idx) => {
      if (idx === eventIndex) return line.replace("hello ", "hacked ");
      return line;
    })
    .join("\n") + "\n";

  const { store } = createStore();
  await assert.rejects(
    () => store.importArchive("s", Readable.from(mutated)),
    (error: unknown) =>
      error instanceof ArchiveIntegrityError && error.reason === "hash-mismatch",
  );

  assert.equal(store.getSnapshot("s").revision, 0);
  assert.equal(store.getCorrectionLineage("s", "a", 0).length, 0);
});

test("truncated archive fails before any state is visible", async () => {
  const source = seededSession();
  const archive = await collectArchive(source.exportArchive("s"));
  const lines = archive.split("\n").filter((l) => l.length > 0);
  const truncated = lines.slice(0, lines.length - 2).join("\n") + "\n";

  const { store } = createStore();
  await assert.rejects(
    () => store.importArchive("s", Readable.from(truncated)),
    (error: unknown) =>
      error instanceof ArchiveIntegrityError && error.reason === "truncated",
  );
  assert.equal(store.getSnapshot("s").revision, 0);
});

test("reordered data lines fail hash verification", async () => {
  const source = seededSession();
  const archive = await collectArchive(source.exportArchive("s"));
  const lines = archive.split("\n").filter((l) => l.length > 0);
  const header = lines[0] as string;
  const footer = lines[lines.length - 1] as string;
  const body = lines.slice(1, -1);
  [body[1], body[2]] = [body[2] as string, body[1] as string];
  const reordered = [header, ...body, footer].join("\n") + "\n";

  const { store } = createStore();
  await assert.rejects(
    () => store.importArchive("s", Readable.from(reordered)),
    (error: unknown) =>
      error instanceof ArchiveIntegrityError && error.reason === "hash-mismatch",
  );
});

test("archive with conflicting existing data is rejected and leaves state intact", async () => {
  const source = seededSession();
  const archive = await collectArchive(source.exportArchive("s"));

  const { store } = createStore();
  store.ingest("s", {
    eventId: "e1",
    sourceId: "a",
    sourceSeq: 0,
    isFinal: true,
    text: "different content",
  });
  const before = store.getSnapshot("s");

  await assert.rejects(
    () => store.importArchive("s", Readable.from(archive)),
    (error: unknown) => error instanceof ArchiveImportConflictError,
  );

  assert.deepEqual(store.getSnapshot("s"), before);
});

test("fault injection mid-import rolls back without leaving partial session", async () => {
  const source = seededSession();
  const archive = await collectArchive(source.exportArchive("s"));
  const totalDataLines =
    archive.split("\n").filter((l) => l.length > 0).length - 2;

  for (const failAt of [1, Math.max(1, Math.floor(totalDataLines / 2)), totalDataLines - 1]) {
    const { store } = createStore();
    await assert.rejects(
      () =>
        store.importArchive("s", Readable.from(archive), {
          injectFailureAfterRecords: failAt,
        }),
      /injected import failure/,
    );
    assert.equal(
      store.getSnapshot("s").revision,
      0,
      `expected empty session after failure at record ${failAt}`,
    );
    assert.equal(store.getCorrectionLineage("s", "a", 0).length, 0);
  }
});

test("archive file round-trip is equivalent", async () => {
  const source = seededSession();
  const dir = tempPath();
  const path = join(dir, "session.asr-archive");
  await source.writeArchive("s", path);

  const raw = readFileSync(path, "utf8");
  const lines = raw.split("\n").filter((l) => l.length > 0);
  const header = JSON.parse(lines[0] as string) as {
    magic: string;
    archiveVersion: number;
    recordCount: number;
  };
  assert.equal(header.magic, "ASR-SESSION-ARCHIVE");
  assert.equal(header.archiveVersion, 1);
  assert.equal(header.recordCount, lines.length - 2);

  const { store } = createStore();
  await store.importArchiveFile("s", path);
  assert.deepEqual(store.getSnapshot("s"), source.getSnapshot("s"));
});

test("unknown optional fields on known records are preserved round-trip", async () => {
  const source = seededSession();
  const archive = await collectArchive(source.exportArchive("s"));
  const lines = archive.split("\n").filter((l) => l.length > 0);

  const eventIdx = lines.findIndex((l) => l.includes('"eventId":"e1"'));
  assert.ok(eventIdx > 0);
  const eventLine = JSON.parse(lines[eventIdx] as string) as Record<string, unknown>;
  (eventLine.data as Record<string, unknown>).confidence = 0.98;
  (eventLine.data as Record<string, unknown>).diagInfo = {
    engine: "future",
    flags: ["a", "b"],
  };

  const mutatedLines = [...lines];
  mutatedLines[eventIdx] = JSON.stringify(eventLine);

  const { store: first } = createStore();
  await assert.rejects(
    () => first.importArchive("s", Readable.from(mutatedLines.join("\n") + "\n")),
    (error: unknown) =>
      error instanceof ArchiveIntegrityError && error.reason === "hash-mismatch",
  );

  const crypto = await import("node:crypto");
  const body = mutatedLines.slice(1, -1);
  const hash = crypto.createHash("sha256");
  for (const line of body) {
    hash.update(line);
    hash.update("\n");
  }
  const dataHash = hash.digest("hex");

  const header = JSON.parse(lines[0] as string) as Record<string, unknown>;
  header.dataHash = dataHash;
  const footer = JSON.parse(lines[lines.length - 1] as string) as Record<string, unknown>;
  footer.dataHash = dataHash;

  const valid = [
    JSON.stringify(header),
    ...body,
    JSON.stringify(footer),
  ].join("\n") + "\n";

  const { store } = createStore();
  await store.importArchive("s", Readable.from(valid));
  const reExport = await collectArchive(store.exportArchive("s"));
  const reLines = reExport.split("\n").filter((l) => l.length > 0);
  const reEvent = JSON.parse(reLines[eventIdx] as string) as {
    data: Record<string, unknown>;
  };
  assert.equal(reEvent.data.confidence, 0.98);
  assert.deepEqual(reEvent.data.diagInfo, {
    engine: "future",
    flags: ["a", "b"],
  });
  assert.equal(reEvent.data.text, "hello ");
});

test("unknown record types are preserved verbatim across round-trip", async () => {
  const source = seededSession();
  const archive = await collectArchive(source.exportArchive("s"));
  const lines = archive.split("\n").filter((l) => l.length > 0);

  const futureRecord = {
    type: "futureSegmentMetric",
    data: {
      metricId: "m-1",
      values: [1, 2, 3],
      note: "added by future version",
    },
  };
  const body = [...lines.slice(1, -1), JSON.stringify(futureRecord)];

  const crypto = await import("node:crypto");
  const hash = crypto.createHash("sha256");
  for (const line of body) {
    hash.update(line);
    hash.update("\n");
  }
  const dataHash = hash.digest("hex");

  const header = JSON.parse(lines[0] as string) as Record<string, unknown>;
  header.dataHash = dataHash;
  header.recordCount = body.length;
  const footer = { end: true, recordCount: body.length, dataHash };

  const valid = [
    JSON.stringify(header),
    ...body,
    JSON.stringify(footer),
  ].join("\n") + "\n";

  const { store } = createStore();
  const result = await store.importArchive("s", Readable.from(valid));
  assert.equal(result.imported.unknown, 1);

  const reExport = await collectArchive(store.exportArchive("s"));
  const reLines = reExport.split("\n").filter((l) => l.length > 0);
  const found = reLines.find((l) => l.includes('"futureSegmentMetric"'));
  assert.ok(found, "unknown record type must survive export");
  assert.deepEqual(JSON.parse(found as string), futureRecord);
});

test("consumer resume position after import is equivalent", async () => {
  const source = seededSession();
  source.acknowledge("s", "slow", 2);
  const archive = await collectArchive(source.exportArchive("s"));

  const { store } = createStore();
  await store.importArchive("s", Readable.from(archive));

  assert.equal(store.getCursor("s", "slow"), 2);
  const slowConsumer = store.createConsumer("s", "slow");
  const changes = slowConsumer.fetch();
  assert.ok(changes.length > 0);
  assert.ok(changes.every((c) => c.revision > 2));
});
