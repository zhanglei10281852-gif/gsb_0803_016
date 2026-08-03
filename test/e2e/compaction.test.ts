import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { join } from "node:path";
import { RevisionHub, ResetRequiredError } from "../../src/index";
import { tempDbPath } from "../helpers";

const COMPACT_WORKER = join(process.cwd(), "build", "test", "e2e", "compaction-worker.js");

function spawnWorker(dbPath: string, sessionId: string, passes: number, sleepMs: number) {
  return new Promise<{ passes: number; reclaimedTotal: number; maxWatermark: number }>(
    (resolve, reject) => {
      const child = spawn(
        process.execPath,
        [COMPACT_WORKER, dbPath, sessionId, String(passes), String(sleepMs)],
        { stdio: ["ignore", "pipe", "inherit"] },
      );
      let buf = "";
      child.stdout.on("data", (d: Buffer) => (buf += d.toString()));
      child.on("exit", (code) =>
        code === 0 ? resolve(JSON.parse(buf.trim().split("\n").filter(Boolean).pop()!)) : reject(new Error(`exit ${code}`)),
      );
      child.on("error", reject);
    },
  );
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test("concurrent compaction while a writer streams and a leased consumer reads", async () => {
  const sessionId = "long";
  const { path, cleanup } = tempDbPath();
  const total = 400;
  try {
    // Seed a little so there is something to lease/checkpoint immediately.
    const setup = RevisionHub.open({ path, busyTimeoutMs: 20000 });
    setup.apply({ sessionId, sourceId: "a", segmentId: "g0", eventId: "e0", sourceSeq: 1, kind: "final", text: "seg 0" });
    // A live-lease consumer that will keep reading and acking.
    setup.acquireConsumerLease({ sessionId, consumerId: "reader", ttlMs: 600_000 });
    setup.close();

    // Writer: append revisions in-process asynchronously.
    const writer = RevisionHub.open({ path, busyTimeoutMs: 20000 });
    let produced = 1;
    const produce = (async () => {
      for (let i = 1; i < total; i++) {
        writer.apply({
          sessionId,
          sourceId: "a",
          segmentId: `g${i}`,
          eventId: `e${i}`,
          sourceSeq: i + 1,
          kind: "final",
          text: `seg ${i}`,
          startMs: i * 10,
        });
        produced++;
        if (i % 20 === 0) await sleep(2);
      }
    })();

    // Reader: pulls, acks, and renews its lease as it goes. Must never lose data
    // or hit a reset (its live lease protects unread revisions from compaction).
    const reader = RevisionHub.open({ path, busyTimeoutMs: 20000 });
    const delivered: number[] = [];
    let cursor = 0;
    const consume = (async () => {
      for (let guard = 0; guard < 200000; guard++) {
        reader.renewConsumerLease(sessionId, "reader", 600_000);
        let batch;
        try {
          batch = reader.pull(sessionId, "reader", { limit: 7 });
        } catch (e) {
          assert.fail(`live-lease reader must not be reset: ${(e as Error).message}`);
        }
        if (batch.length === 0) {
          if (produced >= total) break;
          await sleep(3);
          continue;
        }
        for (const r of batch) {
          assert.equal(r.revision, cursor + 1, `no gap: expected ${cursor + 1}, got ${r.revision}`);
          cursor = r.revision;
          delivered.push(r.revision);
        }
        reader.ack(sessionId, "reader", cursor);
        await sleep(2);
      }
    })();

    // Two concurrent compaction processes hammering compact() during writes.
    const compactors = Promise.all([
      spawnWorker(path, sessionId, 40, 3),
      spawnWorker(path, sessionId, 40, 4),
    ]);

    const [, , compactorResults] = await Promise.all([produce, consume, compactors]);

    // The reader received every revision exactly once, in order, despite
    // concurrent compaction.
    assert.equal(delivered.length, total, "reader saw every revision exactly once");
    for (let i = 0; i < total; i++) assert.equal(delivered[i], i + 1);

    // Compaction made progress (some rows were reclaimed) without corrupting the log.
    const totalReclaimed = compactorResults.reduce((a, r) => a + r.reclaimedTotal, 0);
    assert.ok(totalReclaimed > 0, "compaction reclaimed some revisions");

    // Final integrity: retained revisions are exactly (compactedUpto, head], dense.
    const hub = RevisionHub.open({ path, busyTimeoutMs: 20000 });
    try {
      const state = hub.getCompactionState(sessionId);
      const rows = hub.database
        .prepare("SELECT revision FROM revisions WHERE session_id=? ORDER BY revision")
        .all(sessionId) as Array<{ revision: number }>;
      // No revision at or below the watermark remains.
      for (const r of rows) assert.ok(r.revision > state.compactedUpto, "no sub-watermark rows remain");
      // Remaining revisions are contiguous up to head.
      if (rows.length > 0) {
        assert.equal(rows[rows.length - 1]!.revision, state.headRevision);
        for (let i = 1; i < rows.length; i++) {
          assert.equal(rows[i]!.revision, rows[i - 1]!.revision + 1, "retained log is dense");
        }
      }
      // Snapshot still complete: every segment present.
      assert.equal(hub.getSnapshot(sessionId).segments.length, total);
    } finally {
      hub.close();
    }
    writer.close();
    reader.close();
  } finally {
    cleanup();
  }
});

test("concurrent compaction past an expired consumer forces a reset, recovery resumes", async () => {
  const sessionId = "expiring";
  const { path, cleanup } = tempDbPath();
  let now = 10_000_000;
  const hub = RevisionHub.open({ path, busyTimeoutMs: 20000, clock: () => now });
  try {
    for (let i = 0; i < 50; i++) {
      hub.apply({ sessionId, sourceId: "a", segmentId: `g${i}`, eventId: `e${i}`, sourceSeq: i + 1, kind: "final", text: `seg ${i}`, startMs: i * 10 });
    }
    // A consumer that reads a bit then lets its lease lapse.
    hub.acquireConsumerLease({ sessionId, consumerId: "lapsing", ttlMs: 1_000 });
    hub.ack(sessionId, "lapsing", 10);
    now += 5_000; // lease expires

    // Compaction now advances past the lapsed consumer (checkpoint@50).
    const stats = hub.compact(sessionId);
    assert.ok(stats.compactedUpto >= 11, "compaction advanced past lapsed cursor");

    // The lapsed consumer must get a reset, not silent skipping.
    assert.throws(() => hub.pull(sessionId, "lapsing"), (e: unknown) => e instanceof ResetRequiredError);

    // Recover it from the checkpoint and resume with no gaps.
    const rec = hub.recoverConsumer(sessionId, "lapsing");
    assert.equal(rec.snapshot.segments.length, 50);
    // Add more and confirm the recovered consumer reads forward cleanly.
    hub.apply({ sessionId, sourceId: "a", segmentId: "g50", eventId: "e50", sourceSeq: 51, kind: "final", text: "seg 50" });
    const next = hub.pull(sessionId, "lapsing");
    assert.deepEqual(next.map((r) => r.revision), [rec.checkpointRevision + 1]);
  } finally {
    hub.close();
    cleanup();
  }
});
