import { test } from "node:test";
import assert from "node:assert/strict";
import { RevisionHub } from "../../src/index";
import { tempDbPath } from "../helpers";

function hubWithRevisions(n: number) {
  const { path, cleanup } = tempDbPath();
  const hub = RevisionHub.open({ path });
  for (let i = 0; i < n; i++) {
    hub.apply({
      sessionId: "s",
      sourceId: "a",
      segmentId: `g-${i}`,
      eventId: `e-${i}`,
      sourceSeq: 1,
      kind: "partial",
      text: `t${i}`,
    });
  }
  return { hub, cleanup: () => (hub.close(), cleanup()) };
}

test("pull returns from cursor and ack advances durably", () => {
  const { hub, cleanup } = hubWithRevisions(10);
  try {
    const first = hub.pull("s", "c1", { limit: 4 });
    assert.deepEqual(first.map((r) => r.revision), [1, 2, 3, 4]);

    // No ack yet → same batch is re-delivered (nothing lost).
    const again = hub.pull("s", "c1", { limit: 4 });
    assert.deepEqual(again.map((r) => r.revision), [1, 2, 3, 4]);

    // Ack up to 4; next pull continues after it.
    assert.equal(hub.ack("s", "c1", 4), 4);
    const next = hub.pull("s", "c1", { limit: 4 });
    assert.deepEqual(next.map((r) => r.revision), [5, 6, 7, 8]);

    hub.ack("s", "c1", 8);
    const tail = hub.pull("s", "c1");
    assert.deepEqual(tail.map((r) => r.revision), [9, 10]);
    hub.ack("s", "c1", 10);
    assert.deepEqual(hub.pull("s", "c1"), []);
  } finally {
    cleanup();
  }
});

test("ack is monotonic: acking older cursor is a no-op", () => {
  const { hub, cleanup } = hubWithRevisions(5);
  try {
    hub.ack("s", "c1", 3);
    assert.equal(hub.ack("s", "c1", 1), 3, "cursor must not move backwards");
    assert.deepEqual(hub.pull("s", "c1").map((r) => r.revision), [4, 5]);
  } finally {
    cleanup();
  }
});

test("independent consumers keep separate cursors", () => {
  const { hub, cleanup } = hubWithRevisions(6);
  try {
    hub.ack("s", "c1", 6);
    assert.deepEqual(hub.pull("s", "c1"), []);
    // c2 has never acked → sees everything.
    assert.deepEqual(hub.pull("s", "c2").map((r) => r.revision), [1, 2, 3, 4, 5, 6]);
  } finally {
    cleanup();
  }
});

test("acked ranges never re-sent, unacked never skipped (fuzz)", () => {
  const { hub, cleanup } = hubWithRevisions(50);
  try {
    const delivered = new Set<number>();
    let acked = 0;
    // Simulate a flaky consumer: sometimes acks partially, sometimes not.
    for (let round = 0; round < 30; round++) {
      const batch = hub.pull("s", "c1", { limit: 7 });
      for (const r of batch) {
        // No revision below the acked watermark may ever reappear.
        assert.ok(r.revision > acked, `re-sent acked revision ${r.revision} (acked=${acked})`);
        delivered.add(r.revision);
      }
      if (batch.length === 0) break;
      // Ack a prefix of what we just saw (simulate slow/partial commit).
      const ackTo = batch[Math.floor(batch.length / 2)]!.revision;
      acked = hub.ack("s", "c1", ackTo);
    }
    // Drain the rest.
    let tail = hub.pull("s", "c1", { limit: 100 });
    while (tail.length) {
      for (const r of tail) delivered.add(r.revision);
      acked = hub.ack("s", "c1", tail[tail.length - 1]!.revision);
      tail = hub.pull("s", "c1", { limit: 100 });
    }
    // Everything from 1..50 must have been delivered exactly (set covers all).
    for (let i = 1; i <= 50; i++) assert.ok(delivered.has(i), `missing revision ${i}`);
  } finally {
    cleanup();
  }
});

test("afterRevision overrides cursor without moving it", () => {
  const { hub, cleanup } = hubWithRevisions(5);
  try {
    hub.ack("s", "c1", 5);
    const replay = hub.pull("s", "c1", { afterRevision: 0 });
    assert.deepEqual(replay.map((r) => r.revision), [1, 2, 3, 4, 5]);
    // Stored cursor untouched.
    assert.equal(hub.getCursor("s", "c1"), 5);
  } finally {
    cleanup();
  }
});
