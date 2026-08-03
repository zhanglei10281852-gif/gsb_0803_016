import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Readable } from "node:stream";
import { afterEach, test } from "node:test";
import {
  ConsumerResetRequiredError,
  createRecognitionStore,
  RecognitionStore,
  RevisionCompactedError,
} from "../src";
import type { RecognitionEventInput } from "../src";

const stores: RecognitionStore[] = [];
const paths: string[] = [];

function createStore(now?: () => number): {
  store: RecognitionStore;
  filename: string;
} {
  const dir = mkdtempSync(join(tmpdir(), "asr-compact-"));
  paths.push(dir);
  const filename = join(dir, "store.db");
  const store = createRecognitionStore({
    filename,
    busyTimeout: 5000,
    ...(now ? { now } : {}),
  });
  stores.push(store);
  return { store, filename };
}

afterEach(() => {
  while (stores.length > 0) stores.pop()?.close();
  for (const path of paths.splice(0)) {
    try {
      rmSync(path, { recursive: true, force: true });
    } catch {
      // ignore cleanup errors on Windows
    }
  }
});

async function collectArchive(
  stream: AsyncIterable<string>,
): Promise<string> {
  let out = "";
  for await (const chunk of stream) out += chunk;
  return out;
}

function seed(store: RecognitionStore): number {
  const events: RecognitionEventInput[] = [
    { eventId: "e1", sourceId: "a", sourceSeq: 0, isFinal: true, text: "one " },
    { eventId: "e2", sourceId: "a", sourceSeq: 1, isFinal: true, text: "two " },
    { eventId: "e3", sourceId: "a", sourceSeq: 2, isFinal: true, text: "three" },
  ];
  const outcomes = store.ingest("s", events) as Array<{ revision: number }>;
  return outcomes[outcomes.length - 1]!.revision;
}

function expectedSnapshot(store: RecognitionStore) {
  return store.getSnapshot("s");
}

test("compaction removes revisions before the slowest active cursor covered by a checkpoint", async () => {
  let clock = 1_000_000;
  const { store } = createStore(() => clock);
  const head = seed(store);
  assert.equal(head, 3);

  // Two consumers; the slow one is at revision 2, the fast one caught up.
  const slow = store.createConsumer("s", "slow");
  const fast = store.createConsumer("s", "fast");
  slow.acknowledge(2);
  fast.acknowledge(3);

  // No checkpoint yet -> compact skipped.
  let result = store.compact("s");
  assert.equal(result.compacted, false);
  assert.equal(result.skipped, "no-checkpoint");

  // Export an archive at the current head and register it as a checkpoint.
  const archive = await collectArchive(store.exportArchive("s"));
  const header = JSON.parse(archive.split("\n")[0] as string) as {
    dataHash: string;
    recordCount: number;
  };
  const cp = store.registerCheckpoint("s", {
    revision: head,
    archiveHash: header.dataHash,
    recordCount: header.recordCount,
  });
  assert.equal(cp.revision, head);

  result = store.compact("s");
  assert.equal(result.compacted, true, "compaction should proceed");
  assert.equal(result.safeWatermark, 2);
  assert.equal(result.removed.revisionsRemoved, 2);
  assert.equal(result.checkpointId, cp.checkpointId);

  const state = store.getCompactionState("s");
  assert.equal(state.compactedThroughRevision, 2);
  assert.equal(state.baselineRevision, 2);
  assert.equal(state.lastCheckpointId, cp.checkpointId);
  assert.ok(state.totalRevisionsRemoved >= 2);

  // Current snapshot is unchanged.
  assert.equal(expectedSnapshot(store).text, "one two three");

  // Accessing a compacted revision throws RevisionCompactedError.
  assert.throws(
    () => store.getRevision("s", 1),
    (e: unknown) =>
      e instanceof RevisionCompactedError &&
      e.requestedRevision === 1 &&
      e.baselineRevision === 2,
  );

  // Revision 3 still available.
  assert.ok(store.getRevision("s", 3));

  // Expired consumer (lease past TTL) is not considered, but slow consumer
  // is active because we just touched it via acknowledge/fetch.
});

test("expired consumer gets reset-required and can resume from checkpoint", async () => {
  let clock = 2_000_000;
  const { store } = createStore(() => clock);
  const head = seed(store);

  const stale = store.createConsumer("s", "stale");
  stale.heartbeat(1000);
  stale.acknowledge(1);

  // Advance clock beyond the consumer lease TTL.
  clock += 5000;

  // The other consumer is caught up.
  const active = store.createConsumer("s", "active");
  active.heartbeat(60000);
  active.acknowledge(head);

  // Register a checkpoint covering the head.
  const archive = await collectArchive(store.exportArchive("s"));
  const header = JSON.parse(archive.split("\n")[0] as string) as {
    dataHash: string;
    recordCount: number;
  };
  const cp = store.registerCheckpoint("s", {
    revision: head,
    archiveHash: header.dataHash,
    recordCount: header.recordCount,
  });

  // Since the stale consumer is expired, only active counts; safe watermark = head.
  const result = store.compact("s");
  assert.equal(result.compacted, true);
  assert.equal(result.safeWatermark, head);

  // The stale consumer now tries to fetch and must get reset-required.
  assert.throws(
    () => stale.fetch(),
    (e: unknown) =>
      e instanceof ConsumerResetRequiredError &&
      e.checkpointId === cp.checkpointId &&
      e.checkpointRevision === head,
  );

  // After resetting to the checkpoint, the consumer resumes without error.
  const newCursor = stale.resetToCheckpoint(cp.checkpointId);
  assert.equal(newCursor, head);
  const after = stale.fetch();
  assert.deepEqual(after, []);
  assert.equal(stale.getCursor(), head);
});

test("compaction advances only through the slowest active cursor", async () => {
  let clock = 3_000_000;
  const { store } = createStore(() => clock);
  const head = seed(store);

  const behind = store.createConsumer("s", "behind");
  behind.heartbeat(60000);
  behind.acknowledge(1);

  const caught = store.createConsumer("s", "caught");
  caught.heartbeat(60000);
  caught.acknowledge(head);

  const archive = await collectArchive(store.exportArchive("s"));
  const header = JSON.parse(archive.split("\n")[0] as string) as {
    dataHash: string;
    recordCount: number;
  };
  store.registerCheckpoint("s", {
    revision: head,
    archiveHash: header.dataHash,
    recordCount: header.recordCount,
  });

  const result = store.compact("s");
  assert.equal(result.compacted, true);
  assert.equal(result.safeWatermark, 1);
  assert.equal(result.removed.revisionsRemoved, 1);
  assert.throws(
    () => store.getRevision("s", 1),
    (e: unknown) => e instanceof RevisionCompactedError,
  );
  assert.ok(store.getRevision("s", 2));
  assert.ok(store.getRevision("s", 3));

  // The active behind consumer can continue reading revisions 2 and 3.
  assert.deepEqual(
    behind
      .fetch()
      .map((revision) => revision.revision),
    [2, 3],
  );
});

test("post-compaction archive is verifiable and reconstructs the same snapshot", async () => {
  let clock = 4_000_000;
  const { store: source } = createStore(() => clock);
  const head = seed(source);

  // Add a correction so lineage is present.
  const claim = source.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "alice",
    baseRevision: head,
    ttlMs: 60000,
  });
  source.submitCorrection("s", {
    leaseId: claim.lease.leaseId,
    actor: "alice",
    reason: "spacing",
    correctedText: "One ",
    baseRevision: head,
  });
  const correctedHead = source.getSnapshot("s").revision;

  const consumer = source.createConsumer("s", "qc");
  consumer.heartbeat(60000);
  consumer.acknowledge(correctedHead);

  const firstArchive = await collectArchive(source.exportArchive("s"));
  const firstHeader = JSON.parse(firstArchive.split("\n")[0] as string) as {
    dataHash: string;
    recordCount: number;
  };
  const cp1 = source.registerCheckpoint("s", {
    revision: correctedHead,
    archiveHash: firstHeader.dataHash,
    recordCount: firstHeader.recordCount,
  });

  const compactResult = source.compact("s");
  assert.equal(compactResult.compacted, true);

  // Add more events after compaction.
  source.ingest("s", {
    eventId: "e4",
    sourceId: "a",
    sourceSeq: 3,
    isFinal: true,
    text: " four",
  });
  const finalHead = source.getSnapshot("s").revision;

  // Export again after compaction. The archive must include checkpoints and
  // compaction state, and be importable into an empty store.
  const secondArchive = await collectArchive(source.exportArchive("s"));

  const { store: target } = createStore(() => clock);
  const imported = await target.importArchive(
    "s",
    Readable.from(secondArchive),
  );
  assert.ok(imported.imported.checkpoints >= 1);

  // Snapshot equivalence.
  assert.deepEqual(target.getSnapshot("s"), source.getSnapshot("s"));
  assert.equal(target.getSnapshot("s").text, "One two three four");

  // Checkpoint and compaction state survived import.
  assert.deepEqual(target.getCheckpoint("s", cp1.checkpointId)?.revision, cp1.revision);
  const importedState = target.getCompactionState("s");
  assert.equal(importedState.baselineRevision, correctedHead);
  assert.equal(importedState.lastCheckpointId, cp1.checkpointId);

  // Correction lineage is intact.
  const lineage = target.getCorrectionLineage("s", "a", 0);
  assert.equal(lineage.length, 1);
  assert.equal(lineage[0]!.actor, "alice");
  assert.equal(lineage[0]!.correctedText, "One ");

  // Old revisions below the baseline are gone on both sides.
  assert.throws(
    () => target.getRevision("s", 1),
    (e: unknown) => e instanceof RevisionCompactedError,
  );
  assert.ok(target.getRevision("s", finalHead));
});

test("concurrent writes and compaction leave a consistent snapshot", async () => {
  let clock = 5_000_000;
  const { store } = createStore(() => clock);
  const head = seed(store);

  const consumer = store.createConsumer("s", "writer");
  consumer.heartbeat(60000);
  consumer.acknowledge(head);

  const archive = await collectArchive(store.exportArchive("s"));
  const header = JSON.parse(archive.split("\n")[0] as string) as {
    dataHash: string;
    recordCount: number;
  };
  store.registerCheckpoint("s", {
    revision: head,
    archiveHash: header.dataHash,
    recordCount: header.recordCount,
  });

  // Simulate a writer racing with compaction by inserting an event *after*
  // the checkpoint was registered but before/during compact. The write happens
  // in a separate immediate transaction; compact uses the slowest active
  // cursor (still at head), so it may compact through head while the new
  // event gets revision head+1. The resulting snapshot must still be correct.
  store.ingest("s", {
    eventId: "late",
    sourceId: "a",
    sourceSeq: 99,
    isFinal: true,
    text: " late",
  });
  // Advance the consumer so the new revision is also covered next time, but
  // for this compact call safe watermark remains head.
  consumer.acknowledge(head + 1);

  const result = store.compact("s");
  assert.equal(result.compacted, true);
  assert.equal(result.safeWatermark, head);

  const snapshot = store.getSnapshot("s");
  assert.ok(snapshot.text.includes("late"));
  assert.equal(snapshot.revision, head + 1);
  // The new revision is still queryable.
  assert.equal(store.getRevision("s", head + 1).revision, head + 1);
});

test("dry-run compaction reports what would be removed without deleting", async () => {
  let clock = 6_000_000;
  const { store } = createStore(() => clock);
  const head = seed(store);
  const c = store.createConsumer("s", "c");
  c.heartbeat(60000);
  c.acknowledge(head);

  const archive = await collectArchive(store.exportArchive("s"));
  const header = JSON.parse(archive.split("\n")[0] as string) as {
    dataHash: string;
    recordCount: number;
  };
  store.registerCheckpoint("s", {
    revision: head,
    archiveHash: header.dataHash,
    recordCount: header.recordCount,
  });

  const dry = store.compact("s", { dryRun: true });
  assert.equal(dry.compacted, false);
  assert.ok(dry.removed.revisionsRemoved > 0);
  assert.equal(store.getCompactionState("s").compactedThroughRevision, 0);
  assert.ok(store.getRevision("s", 1));

  const real = store.compact("s");
  assert.equal(real.compacted, true);
  assert.equal(real.removed.revisionsRemoved, dry.removed.revisionsRemoved);
  assert.throws(
    () => store.getRevision("s", 1),
    (e: unknown) => e instanceof RevisionCompactedError,
  );
});
