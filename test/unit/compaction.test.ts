import { test } from "node:test";
import assert from "node:assert/strict";
import {
  RevisionHub,
  ResetRequiredError,
  ArchiveIntegrityError,
  type Snapshot,
} from "../../src/index";
import { tempDbPath } from "../helpers";

/** Hub with a controllable clock so lease TTLs expire deterministically. */
function controllableHub() {
  const { path, cleanup } = tempDbPath();
  let now = 1_000_000;
  const hub = RevisionHub.open({ path, clock: () => now });
  return {
    hub,
    advance: (ms: number) => (now += ms),
    cleanup: () => (hub.close(), cleanup()),
  };
}

/** Emit N finalized single-source segments; returns the head revision. */
function seed(hub: RevisionHub, sessionId: string, n: number): number {
  for (let i = 0; i < n; i++) {
    hub.apply({
      sessionId,
      sourceId: "a",
      segmentId: `g${i}`,
      eventId: `e${i}`,
      sourceSeq: i + 1,
      kind: "final",
      text: `seg ${i}`,
      startMs: i * 100,
      endMs: i * 100 + 50,
    });
  }
  return hub.headRevision(sessionId);
}

function fp(snap: Snapshot): string {
  return JSON.stringify({ head: snap.headRevision, segments: snap.segments, summary: snap.summary });
}

test("compaction reclaims only below the slowest live cursor AND a checkpoint", () => {
  const { hub, cleanup } = controllableHub();
  try {
    seed(hub, "s", 20);
    // Two live-lease consumers with different progress.
    hub.acquireConsumerLease({ sessionId: "s", consumerId: "fast", ttlMs: 60_000 });
    hub.acquireConsumerLease({ sessionId: "s", consumerId: "slow", ttlMs: 60_000 });
    hub.ack("s", "fast", 15);
    hub.ack("s", "slow", 8); // slowest live cursor = 8

    const stats = hub.compact("s"); // creates a checkpoint at head=20
    assert.equal(stats.checkpointCreated, true);
    assert.equal(stats.checkpointRevision, 20);
    assert.equal(stats.slowestLiveCursor, 8);
    // Watermark = min(20, 8) = 8 → revisions 1..8 reclaimed.
    assert.equal(stats.compactedUpto, 8);
    assert.equal(stats.reclaimed, 8);
    assert.equal(hub.getCompactionState("s").retained, 12);
  } finally {
    cleanup();
  }
});

test("without a checkpoint nothing is reclaimed even below the slowest cursor", () => {
  const { hub, cleanup } = controllableHub();
  try {
    seed(hub, "s", 10);
    hub.acquireConsumerLease({ sessionId: "s", consumerId: "c", ttlMs: 60_000 });
    hub.ack("s", "c", 6);
    // checkpoint:false → no checkpoint exists → checkpointRevision 0 → no reclaim.
    const stats = hub.compact("s", { checkpoint: false });
    assert.equal(stats.checkpointRevision, 0);
    assert.equal(stats.reclaimed, 0);
    assert.equal(stats.compactedUpto, 0);
  } finally {
    cleanup();
  }
});

test("a live lease protects unread revisions from reclamation", () => {
  const { hub, cleanup } = controllableHub();
  try {
    seed(hub, "s", 30);
    hub.acquireConsumerLease({ sessionId: "s", consumerId: "c", ttlMs: 60_000 });
    hub.ack("s", "c", 5);
    const stats = hub.compact("s");
    // Cannot reclaim past 5 while the lease is live, despite checkpoint at 30.
    assert.equal(stats.compactedUpto, 5);
    // The consumer can still read from 6 onward with no reset.
    const batch = hub.pull("s", "c", { limit: 3 });
    assert.deepEqual(batch.map((r) => r.revision), [6, 7, 8]);
  } finally {
    cleanup();
  }
});

test("expired consumer no longer protects; compaction advances past it", () => {
  const { hub, advance, cleanup } = controllableHub();
  try {
    seed(hub, "s", 20);
    hub.acquireConsumerLease({ sessionId: "s", consumerId: "c", ttlMs: 1_000 });
    hub.ack("s", "c", 4);
    advance(2_000); // lease expires
    const stats = hub.compact("s");
    // No live leases → checkpoint (head=20) alone bounds reclamation.
    assert.equal(stats.slowestLiveCursor, null);
    assert.equal(stats.compactedUpto, 20);
    assert.equal(hub.getCompactionState("s").expiredConsumers.includes("c"), true);
  } finally {
    cleanup();
  }
});

test("expired consumer re-reading below the watermark gets ResetRequiredError", () => {
  const { hub, advance, cleanup } = controllableHub();
  try {
    seed(hub, "s", 20);
    hub.acquireConsumerLease({ sessionId: "s", consumerId: "c", ttlMs: 1_000 });
    hub.ack("s", "c", 4);
    advance(2_000);
    hub.compact("s"); // compactedUpto → 20
    let thrown: unknown;
    try {
      hub.pull("s", "c"); // cursor 4 < watermark 20
    } catch (e) {
      thrown = e;
    }
    assert.ok(thrown instanceof ResetRequiredError, "must signal reset, not skip");
    const err = thrown as ResetRequiredError;
    assert.equal(err.code, "RESET_REQUIRED");
    assert.equal(err.details?.compactedUpto, 20);
    assert.equal(err.details?.resumeFrom, 20);
  } finally {
    cleanup();
  }
});

test("recovery from checkpoint rebuilds an equivalent snapshot and resumes", () => {
  const { hub, advance, cleanup } = controllableHub();
  try {
    seed(hub, "s", 20);
    const uncompactedSnap = fp(hub.getSnapshot("s"));

    hub.acquireConsumerLease({ sessionId: "s", consumerId: "c", ttlMs: 1_000 });
    hub.ack("s", "c", 4);
    advance(2_000);
    hub.compact("s"); // checkpoint@20, compactedUpto=20

    // Add more revisions after the checkpoint.
    hub.apply({ sessionId: "s", sourceId: "a", segmentId: "g20", eventId: "e20", sourceSeq: 21, kind: "final", text: "seg 20", startMs: 2000 });

    // Consumer resets → gets checkpoint snapshot equivalent to uncompacted@20.
    const rec = hub.recoverConsumer("s", "c");
    assert.equal(rec.checkpointRevision, 20);
    assert.equal(fp(rec.snapshot), uncompactedSnap, "checkpoint snapshot equals uncompacted");

    // Resumes strictly after the checkpoint — no silent skip, no re-read of gone data.
    const next = hub.pull("s", "c");
    assert.deepEqual(next.map((r) => r.revision), [21]);
  } finally {
    cleanup();
  }
});

test("snapshot is identical before and after compaction", () => {
  const { hub, cleanup } = controllableHub();
  try {
    seed(hub, "s", 25);
    const before = fp(hub.getSnapshot("s"));
    hub.acquireConsumerLease({ sessionId: "s", consumerId: "c", ttlMs: 60_000 });
    hub.ack("s", "c", 25);
    hub.compact("s");
    assert.ok(hub.getCompactionState("s").compactedUpto > 0, "something was compacted");
    assert.equal(fp(hub.getSnapshot("s")), before, "snapshot unchanged by compaction");
  } finally {
    cleanup();
  }
});

test("archives exported before and after compaction both verify and re-import equal", () => {
  const { hub, cleanup } = controllableHub();
  try {
    seed(hub, "s", 15);
    const preArchive = hub.exportSessionToString("s");

    hub.acquireConsumerLease({ sessionId: "s", consumerId: "c", ttlMs: 60_000 });
    hub.ack("s", "c", 15);
    hub.compact("s");
    const postArchive = hub.exportSessionToString("s");

    // Both verify by round-tripping into empty DBs.
    const preT = tempDbPath();
    const postT = tempDbPath();
    const preHub = RevisionHub.open({ path: preT.path });
    const postHub = RevisionHub.open({ path: postT.path });
    try {
      const preRes = preHub.importSession(preArchive);
      const postRes = postHub.importSession(postArchive);
      assert.equal(preRes.imported, true);
      assert.equal(postRes.imported, true);
      // The reconstructed snapshot is identical either way (segments survive
      // compaction; only the revision log shrinks).
      assert.equal(fp(preHub.getSnapshot("s")), fp(postHub.getSnapshot("s")));
      // The post-compaction import carries the watermark forward.
      const postState = postHub.getCompactionState("s");
      assert.ok(postState.compactedUpto > 0, "watermark carried into archive");
    } finally {
      preHub.close(); postHub.close(); preT.cleanup(); postT.cleanup();
    }
  } finally {
    cleanup();
  }
});

test("compaction refuses to trust a tampered checkpoint archive", () => {
  const { hub, cleanup } = controllableHub();
  try {
    seed(hub, "s", 12);
    hub.acquireConsumerLease({ sessionId: "s", consumerId: "c", ttlMs: 60_000 });
    hub.ack("s", "c", 12);
    hub.createCheckpoint("s");
    // Tamper the stored checkpoint archive directly in the DB.
    hub.database
      .prepare("UPDATE checkpoints SET archive = archive || 'garbage' WHERE session_id='s'")
      .run();
    const stats = hub.compact("s", { checkpoint: false });
    // The tampered checkpoint no longer verifies → cannot gate reclamation.
    assert.equal(stats.checkpointRevision, 0);
    assert.equal(stats.reclaimed, 0);
  } finally {
    cleanup();
  }
});

test("checkpoint archive itself verifies and recovery re-verifies it", () => {
  const { hub, cleanup } = controllableHub();
  try {
    seed(hub, "s", 8);
    const cp = hub.createCheckpoint("s");
    assert.equal(cp.revision, 8);
    // A corrupted checkpoint makes recovery fail loudly rather than silently.
    hub.database.prepare("UPDATE checkpoints SET archive='{\"type\":\"header\"}' WHERE session_id='s'").run();
    assert.throws(() => hub.recoverConsumer("s", "c"), (e: unknown) => e instanceof ArchiveIntegrityError);
  } finally {
    cleanup();
  }
});

test("repeated compaction is monotonic and idempotent when nothing new qualifies", () => {
  const { hub, cleanup } = controllableHub();
  try {
    seed(hub, "s", 10);
    hub.acquireConsumerLease({ sessionId: "s", consumerId: "c", ttlMs: 60_000 });
    hub.ack("s", "c", 10);
    const first = hub.compact("s");
    assert.equal(first.compactedUpto, 10);
    const second = hub.compact("s");
    assert.equal(second.reclaimed, 0, "nothing new to reclaim");
    assert.equal(second.compactedUpto, 10, "watermark stays");
  } finally {
    cleanup();
  }
});
