import { describe, expect, it } from 'vitest';
import { ConflictError, TranscriptStore } from '../src';
import { generateSession, makeDeliveries, mulberry32, tmpDbPath } from './helpers';

describe('persistence across restarts', () => {
  it('state, revisions and cursors survive close + reopen', () => {
    const file = tmpDbPath();
    const bySource = generateSession(mulberry32(3), 2, 4);
    const deliveries = makeDeliveries(mulberry32(4), bySource);
    const half = Math.floor(deliveries.length / 2);

    const first = new TranscriptStore(file);
    for (const d of deliveries.slice(0, half)) first.ingest('sess', d.sourceId, d.event);
    const revBefore = first.lastRevision('sess');
    first.ack('consumer', 'sess', revBefore);
    const snapBefore = first.snapshot('sess');
    first.close();

    const second = new TranscriptStore(file);
    expect(second.snapshot('sess')).toEqual(snapBefore);
    expect(second.cursor('consumer', 'sess')).toBe(revBefore);
    // no redelivery of the acked prefix after restart
    expect(second.poll('consumer', 'sess', 10000).entries).toHaveLength(0);

    // revisions continue strictly increasing after restart
    for (const d of deliveries.slice(half)) second.ingest('sess', d.sourceId, d.event);
    const { entries } = second.poll('late-joiner', 'sess', 10000);
    expect(entries[0].revision).toBe(1);
    expect(entries.at(-1)!.revision).toBe(second.lastRevision('sess'));
    expect(entries.length).toBe(second.lastRevision('sess'));
    expect(second.lastRevision('sess')).toBeGreaterThan(revBefore);
    second.close();
  });

  it('duplicate/conflict memory survives reopen', () => {
    const file = tmpDbPath();
    const a = new TranscriptStore(file);
    a.ingest('sess', 'mic', { eventId: 'x', sourceSeq: 0, kind: 'final', text: 'hi', startMs: 0 });
    a.close();

    const b = new TranscriptStore(file);
    const same = { eventId: 'x', sourceSeq: 0, kind: 'final' as const, text: 'hi', startMs: 0 };
    expect(b.ingest('sess', 'mic', same).status).toBe('duplicate');
    expect(() =>
      b.ingest('sess', 'mic', { eventId: 'x', sourceSeq: 1, kind: 'final', text: 'changed' }),
    ).toThrow(ConflictError);
    b.close();
  });
});
