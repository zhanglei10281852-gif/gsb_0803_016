import { test } from "node:test";
import assert from "node:assert/strict";
import { RevisionHub } from "../../src/index";
import { tempDbPath } from "../helpers";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test("slow consumer loses nothing while a producer streams concurrently", async () => {
  const { path, cleanup } = tempDbPath();
  const sessionId = "stream";
  const producer = RevisionHub.open({ path, busyTimeoutMs: 15000 });
  const consumer = RevisionHub.open({ path, busyTimeoutMs: 15000 });
  const total = 300;

  try {
    // Producer appends revisions asynchronously.
    let produced = 0;
    const produce = (async () => {
      for (let i = 0; i < total; i++) {
        producer.apply({
          sessionId,
          sourceId: `src-${i % 3}`,
          segmentId: `seg-${i}`,
          eventId: `e-${i}`,
          sourceSeq: 1,
          kind: "final",
          text: `line ${i}`,
        });
        produced += 1;
        if (i % 25 === 0) await sleep(2); // let the consumer interleave
      }
    })();

    // Slow consumer: small batches, sleeps between, acks each batch durably.
    const delivered: number[] = [];
    let cursor = 0;
    const consume = (async () => {
      // Keep going until producer done AND we've drained everything.
      // Guard against infinite loop with a generous cap.
      for (let guard = 0; guard < 100000; guard++) {
        const batch = consumer.pull(sessionId, "slow", { limit: 5 });
        if (batch.length === 0) {
          if (produced >= total) break;
          await sleep(3);
          continue;
        }
        for (const r of batch) {
          // Strict monotonic, no gaps, no duplicates.
          assert.equal(r.revision, cursor + 1, `expected ${cursor + 1}, got ${r.revision}`);
          cursor = r.revision;
          delivered.push(r.revision);
        }
        consumer.ack(sessionId, "slow", cursor);
        await sleep(4); // deliberately slow
      }
    })();

    await Promise.all([produce, consume]);

    assert.equal(delivered.length, total, "consumer must receive every revision exactly once");
    for (let i = 0; i < total; i++) assert.equal(delivered[i], i + 1);
    assert.equal(consumer.getCursor(sessionId, "slow"), total);
  } finally {
    producer.close();
    consumer.close();
    cleanup();
  }
});

test("consumer restart resumes exactly at the durable cursor (no loss, no dup)", () => {
  const { path, cleanup } = tempDbPath();
  const sessionId = "restart";
  try {
    // Seed 40 revisions.
    {
      const hub = RevisionHub.open({ path });
      for (let i = 0; i < 40; i++) {
        hub.apply({
          sessionId,
          sourceId: "a",
          segmentId: `seg-${i}`,
          eventId: `e-${i}`,
          sourceSeq: 1,
          kind: "final",
          text: `t${i}`,
        });
      }
      hub.close();
    }

    const seen: number[] = [];
    // Session 1: consume 18, ack, then "crash" (drop the handle).
    {
      const hub = RevisionHub.open({ path });
      let cursor = 0;
      while (cursor < 18) {
        const batch = hub.pull(sessionId, "c", { limit: 5 });
        for (const r of batch) {
          if (r.revision > 18) break;
          seen.push(r.revision);
          cursor = r.revision;
        }
        hub.ack(sessionId, "c", cursor);
      }
      hub.close();
    }

    // Session 2: brand new handle → must resume right after the durable cursor.
    {
      const hub = RevisionHub.open({ path });
      assert.equal(hub.getCursor(sessionId, "c"), 18, "cursor must have survived restart");
      let batch = hub.pull(sessionId, "c", { limit: 100 });
      for (const r of batch) {
        seen.push(r.revision);
      }
      hub.ack(sessionId, "c", batch[batch.length - 1]!.revision);
      hub.close();
    }

    // Full 1..40 delivered exactly once, in order.
    assert.equal(seen.length, 40);
    for (let i = 0; i < 40; i++) assert.equal(seen[i], i + 1);
  } finally {
    cleanup();
  }
});
