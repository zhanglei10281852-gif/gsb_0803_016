import assert from 'node:assert/strict';
import { TranscriptStore } from '../dist/index.js';
import { generateSession, mulberry32, sleep, tmpDbPath } from './lib.mjs';

/**
 * Slow consumer: a fast producer writes hundreds of revisions while a slow
 * consumer polls in small pages with delays, acks incrementally, and even
 * restarts (new process instance) mid-stream. Guarantees under test:
 *  - acked revisions are never redelivered;
 *  - unacked revisions are eventually delivered exactly covering 1..N;
 *  - the ack cursor survives a restart.
 */
export async function main() {
  const file = tmpDbPath('asr-e2e-slow-');
  const bySource = generateSession(mulberry32(11), 2, 5);
  const all = [...bySource.entries()].flatMap(([sourceId, events]) =>
    events.map((event) => ({ sourceId, event })),
  );

  let store = new TranscriptStore(file);
  const delivered = new Set();
  let acked = 0;

  // Redelivery check: poll without ack must return the same prefix again.
  for (const d of all.slice(0, 10)) store.ingest('sess', d.sourceId, d.event);
  const first = store.poll('qc', 'sess', 4);
  const again = store.poll('qc', 'sess', 4);
  assert.deepEqual(
    again.entries.map((e) => e.revision),
    first.entries.map((e) => e.revision),
  );

  let produced = 10;
  let restarted = false;
  for (;;) {
    // Producer runs ahead in bursts.
    while (produced < all.length && produced < acked + 40) {
      const d = all[produced++];
      store.ingest('sess', d.sourceId, d.event);
    }

    const { entries, lastRevision } = store.poll('qc', 'sess', 7);
    for (const e of entries) {
      assert.ok(e.revision > acked, `acked revision ${e.revision} was resent`);
      delivered.add(e.revision);
      acked = store.ack('qc', 'sess', e.revision);
      await sleep(1); // the consumer is slow
    }

    // Simulate a consumer restart halfway through.
    if (!restarted && acked > 0 && acked >= Math.floor(lastRevision / 2) && produced < all.length) {
      restarted = true;
      store.close();
      store = new TranscriptStore(file);
      assert.equal(store.cursor('qc', 'sess'), acked, 'cursor survived restart');
    }

    if (produced >= all.length && acked >= lastRevision) break;
  }

  assert.equal(acked, store.lastRevision('sess'));
  assert.deepEqual([...delivered].sort((a, b) => a - b), Array.from({ length: acked }, (_, i) => i + 1));
  assert.equal(store.poll('qc', 'sess', 10).entries.length, 0);
  store.close();
  console.log(`e2e/slow-consumer: OK (${acked} revisions consumed, restart included)`);
}

if (process.argv[1] && process.argv[1].endsWith('slow-consumer.mjs')) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
