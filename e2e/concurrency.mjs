import assert from 'node:assert/strict';
import { ReferenceModel, TranscriptStore } from '../dist/index.js';
import {
  generateSession,
  makeDeliveries,
  mulberry32,
  sleep,
  tmpDbPath,
} from './lib.mjs';

/**
 * Multiple store instances over the same SQLite file ingest the same event
 * set with interleaved "concurrent" tasks. Final snapshot, segment order,
 * summary and the change multiset must equal the canonical reference model.
 */
export async function main() {
  const file = tmpDbPath('asr-e2e-conc-');
  const bySource = generateSession(mulberry32(2026), 3, 8);
  const model = new ReferenceModel();
  for (const [sourceId, events] of bySource) {
    for (const event of events) model.ingest(sourceId, event);
  }

  const deliveries = makeDeliveries(mulberry32(7), bySource);
  const stores = [new TranscriptStore(file), new TranscriptStore(file), new TranscriptStore(file)];

  // Interleave deliveries across instances; random microtask yields make the
  // interleaving irregular, like concurrent producers would.
  const workers = stores.map(async (store, w) => {
    for (let i = w; i < deliveries.length; i += stores.length) {
      const d = deliveries[i];
      store.ingest('sess', d.sourceId, d.event);
      if (i % 7 === 0) await sleep(0);
    }
  });
  await Promise.all(workers);

  // Snapshots must converge on content; revision counts may differ between
  // interleavings (a partial fresh in one order is stale in another).
  const strip = ({ sessionId, revision, ...rest }) => rest;
  for (const store of stores) {
    assert.deepEqual(strip(store.snapshot('sess')), strip(model.snapshot('sess')));
  }

  // Revisions: contiguous 1..N, committing exactly the model's finals.
  const last = stores[0].lastRevision('sess');
  const { entries } = stores[0].poll('qc', 'sess', 10000);
  assert.deepEqual(
    entries.map((e) => e.revision),
    Array.from({ length: last }, (_, i) => i + 1),
  );
  const key = (c) => `${c.sourceId}/${c.eventId}`;
  const finalsOf = (changes) => changes.filter((c) => c.type === 'final').map(key).sort();
  assert.deepEqual(
    finalsOf(entries.map((e) => e.change)),
    finalsOf(model.applied),
  );

  for (const s of stores) s.close();
  console.log(`e2e/concurrency: OK (${deliveries.length} deliveries, ${entries.length} revisions)`);
}

if (process.argv[1] && process.argv[1].endsWith('concurrency.mjs')) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
