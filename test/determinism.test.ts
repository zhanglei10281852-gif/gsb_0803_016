import { describe, expect, it } from 'vitest';
import { ReferenceModel, TranscriptStore } from '../src';
import type { Snapshot } from '../src';
import { generateSession, makeDeliveries, mulberry32, shuffled } from './helpers';

/**
 * Snapshot equality ignoring sessionId and revision. The revision count may
 * legitimately differ between interleavings (a partial that is fresh in one
 * order is stale in another); the converged content must not.
 */
function comparable(snap: Snapshot): Omit<Snapshot, 'sessionId' | 'revision'> {
  const { sessionId: _a, revision: _b, ...rest } = snap;
  return rest;
}

function canonicalModel(bySource: Map<string, import('../src').AsrEvent[]>): ReferenceModel {
  const model = new ReferenceModel();
  for (const [sourceId, events] of bySource) {
    for (const event of events) model.ingest(sourceId, event);
  }
  return model;
}

describe('convergence / determinism', () => {
  it('any interleaving of the same deliveries converges to the canonical snapshot', () => {
    for (const seed of [1, 7, 42, 1337]) {
      const bySource = generateSession(mulberry32(seed), 3, 6);
      const model = canonicalModel(bySource);

      for (const runSeed of [seed * 3 + 1, seed * 5 + 2]) {
        const rng = mulberry32(runSeed);
        const deliveries = makeDeliveries(rng, bySource);
        const store = new TranscriptStore(':memory:');
        // random batch sizes to simulate different batching of the same stream
        let i = 0;
        while (i < deliveries.length) {
          const batch = deliveries.slice(i, i + 1 + Math.floor(rng() * 5));
          i += batch.length;
          for (const d of batch) store.ingest('sess', d.sourceId, d.event);
        }
        expect(comparable(store.snapshot('sess'))).toEqual(comparable(model.snapshot()));
        // revisions remain contiguous 1..R whatever the interleaving
        const last = store.lastRevision('sess');
        const { entries } = store.poll('c', 'sess', 10000);
        expect(entries.map((e) => e.revision)).toEqual(
          Array.from({ length: last }, (_, i) => i + 1),
        );
        store.close();
      }
    }
  });

  it('revision streams are contiguous and commit the same finals regardless of order', () => {
    const bySource = generateSession(mulberry32(9), 2, 5);
    const model = canonicalModel(bySource);
    const key = (c: { sourceId: string; eventId: string }) => `${c.sourceId}/${c.eventId}`;

    const store = new TranscriptStore(':memory:');
    for (const d of makeDeliveries(mulberry32(99), bySource)) {
      store.ingest('sess', d.sourceId, d.event);
    }
    const last = store.lastRevision('sess');
    const { entries } = store.poll('c', 'sess', 10000);
    expect(entries.map((e) => e.revision)).toEqual(Array.from({ length: last }, (_, i) => i + 1));
    // every final committed by the canonical model is committed exactly once
    const finalsOf = (changes: { type: string; sourceId: string; eventId: string }[]) =>
      changes.filter((c) => c.type === 'final').map(key).sort();
    expect(finalsOf(entries.map((e) => e.change))).toEqual(finalsOf(model.applied));
    store.close();
  });

  it('segment order in the snapshot does not depend on arrival order', () => {
    const bySource = generateSession(mulberry32(5), 4, 4);
    const a = new TranscriptStore(':memory:');
    const b = new TranscriptStore(':memory:');
    const forward = makeDeliveries(mulberry32(11), bySource, 0);
    for (const d of forward) a.ingest('s', d.sourceId, d.event);
    for (const d of shuffled(mulberry32(77), forward)) b.ingest('s', d.sourceId, d.event);
    expect(comparable(b.snapshot('s'))).toEqual(comparable(a.snapshot('s')));
    a.close();
    b.close();
  });
});
