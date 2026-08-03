import { describe, expect, it } from 'vitest';
import { ConflictError, TranscriptStore, ValidationError } from '../src';
import type { AsrEvent } from '../src';

const partial = (seq: number, text: string, id = `e${seq}`): AsrEvent => ({
  eventId: id,
  sourceSeq: seq,
  kind: 'partial',
  text,
  startMs: seq * 10,
});
const final = (seq: number, text: string, id = `e${seq}`): AsrEvent => ({
  eventId: id,
  sourceSeq: seq,
  kind: 'final',
  text,
  startMs: seq * 10,
});

describe('ingest semantics', () => {
  it('applies new events and assigns strictly increasing revisions', () => {
    const s = new TranscriptStore(':memory:');
    const r1 = s.ingest('sess', 'mic', partial(0, 'hel'));
    const r2 = s.ingest('sess', 'mic', partial(1, 'hello'));
    const r3 = s.ingest('sess', 'mic', final(2, 'hello'));
    expect([r1.revision, r2.revision, r3.revision]).toEqual([1, 2, 3]);
    expect(r3.status).toBe('applied');
    s.close();
  });

  it('is idempotent for exact duplicates and assigns no new revision', () => {
    const s = new TranscriptStore(':memory:');
    s.ingest('sess', 'mic', partial(0, 'hel'));
    const dup = s.ingest('sess', 'mic', partial(0, 'hel'));
    expect(dup).toEqual({ status: 'duplicate', revision: 1 });
    expect(s.lastRevision('sess')).toBe(1);
    s.close();
  });

  it('rejects same eventId with different content', () => {
    const s = new TranscriptStore(':memory:');
    s.ingest('sess', 'mic', partial(0, 'hel'));
    expect(() => s.ingest('sess', 'mic', partial(5, 'different', 'e0'))).toThrow(ConflictError);
    // state unchanged after the conflict
    expect(s.lastRevision('sess')).toBe(1);
    s.close();
  });

  it('drops older partials so they cannot overwrite newer state', () => {
    const s = new TranscriptStore(':memory:');
    s.ingest('sess', 'mic', partial(0, 'hel'));
    s.ingest('sess', 'mic', partial(2, 'hello wor'));
    const stale = s.ingest('sess', 'mic', partial(1, 'hello'));
    expect(stale.status).toBe('stale');
    expect(s.snapshot('sess').partials.map((p) => p.text)).toEqual(['hello wor']);
    s.close();
  });

  it('drops a partial with the same seq but a different eventId (no overwrite)', () => {
    const s = new TranscriptStore(':memory:');
    s.ingest('sess', 'mic', partial(1, 'newer', 'a'));
    const r = s.ingest('sess', 'mic', partial(1, 'older-twin', 'b'));
    expect(r.status).toBe('stale');
    expect(s.snapshot('sess').partials[0].text).toBe('newer');
    s.close();
  });

  it('never lets a late partial roll back a confirmed final', () => {
    const s = new TranscriptStore(':memory:');
    s.ingest('sess', 'mic', partial(0, 'hel'));
    s.ingest('sess', 'mic', final(1, 'hello'));
    const late = s.ingest('sess', 'mic', partial(0, 'hel', 'e0'));
    const late2 = s.ingest('sess', 'mic', partial(1, 'hell', 'eX'));
    expect(late.status).not.toBe('applied');
    expect(late2.status).toBe('stale');
    const snap = s.snapshot('sess');
    expect(snap.text).toBe('hello');
    expect(snap.partials).toHaveLength(0);
    s.close();
  });

  it('keeps only the latest partial per source and clears it on final', () => {
    const s = new TranscriptStore(':memory:');
    s.ingest('sess', 'mic', partial(0, 'a'));
    s.ingest('sess', 'mic', partial(1, 'ab'));
    s.ingest('sess', 'mic', partial(2, 'abc'));
    expect(s.snapshot('sess').partials.map((p) => p.text)).toEqual(['abc']);
    s.ingest('sess', 'mic', final(3, 'abc'));
    const snap = s.snapshot('sess');
    expect(snap.partials).toHaveLength(0);
    expect(snap.text).toBe('abc');
    s.close();
  });

  it('tracks sources independently', () => {
    const s = new TranscriptStore(':memory:');
    s.ingest('sess', 'a', final(5, 'from a'));
    // source b starts at its own seq base; not "stale" relative to a
    const r = s.ingest('sess', 'b', partial(0, 'from b'));
    expect(r.status).toBe('applied');
    expect(s.snapshot('sess').summary.sources).toBe(2);
    s.close();
  });

  it('re-delivery of a superseded partial is a harmless stale no-op', () => {
    const s = new TranscriptStore(':memory:');
    s.ingest('sess', 'mic', partial(0, 'a'));
    s.ingest('sess', 'mic', partial(1, 'ab'));
    const r = s.ingest('sess', 'mic', partial(0, 'a')); // row was superseded+deleted
    expect(r.status).toBe('stale');
    expect(s.lastRevision('sess')).toBe(2);
    s.close();
  });

  it('validates input', () => {
    const s = new TranscriptStore(':memory:');
    expect(() => s.ingest('sess', 'mic', { ...partial(0, 'x'), sourceSeq: -1 })).toThrow(
      ValidationError,
    );
    expect(() =>
      s.ingest('sess', 'mic', { ...partial(0, 'x'), kind: 'weird' as never }),
    ).toThrow(ValidationError);
    expect(() => s.ingest('', 'mic', partial(0, 'x'))).toThrow(ValidationError);
    s.close();
  });

  it('ingestBatch is atomic: one conflict rolls back the whole batch', () => {
    const s = new TranscriptStore(':memory:');
    s.ingest('sess', 'mic', final(0, 'committed', 'e0'));
    expect(() =>
      s.ingestBatch('sess', 'mic', [final(1, 'new', 'e1'), final(2, 'x', 'e0')]),
    ).toThrow(ConflictError);
    const snap = s.snapshot('sess');
    expect(snap.text).toBe('committed');
    expect(s.lastRevision('sess')).toBe(1);
    s.close();
  });
});
