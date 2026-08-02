import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTempStore, makeEvent } from '../helpers';
import { RecognitionStore } from '../../src';

describe('snapshot', () => {
  let store: RecognitionStore;
  let cleanup: () => void;

  beforeEach(() => {
    const ctx = createTempStore();
    store = ctx.store;
    cleanup = ctx.cleanup;
  });
  afterEach(() => cleanup());

  it('returns empty snapshot for new session', () => {
    const session = store.session('new');
    const snap = session.getSnapshot();
    expect(snap.sessionId).toBe('new');
    expect(snap.revision).toBe(0);
    expect(snap.text).toBe('');
    expect(snap.sources).toEqual([]);
    expect(snap.summary).toEqual({
      sourceCount: 0,
      fragmentCount: 0,
      finalCount: 0,
      partialCount: 0,
      textLength: 0,
    });
  });

  it('reflects current partial text', () => {
    const session = store.session('s1');
    session.ingest(makeEvent('srcA', 1, 'partial', 'hello'));
    const snap = session.getSnapshot();
    expect(snap.text).toBe('hello');
    expect(snap.summary.partialCount).toBe(1);
    expect(snap.summary.fragmentCount).toBe(1);
  });

  it('snapshot revision increments with each accepted change', () => {
    const session = store.session('s2');
    const r1 = session.ingest(makeEvent('srcA', 1, 'partial', 'a', 'e1'));
    const r2 = session.ingest(makeEvent('srcA', 1, 'partial', 'ab', 'e2'));
    const r3 = session.ingest(makeEvent('srcA', 1, 'final', 'abc', 'e3'));
    expect(r1.revision).toBe(1);
    expect(r2.revision).toBe(2);
    expect(r3.revision).toBe(3);
    expect(session.getSnapshot().revision).toBe(3);
  });

  it('ignored events do not increment snapshot revision', () => {
    const session = store.session('s3');
    session.ingest(makeEvent('srcA', 1, 'final', 'done'));
    session.ingest(makeEvent('srcA', 1, 'partial', 'late'));
    expect(session.getSnapshot().revision).toBe(1);
  });

  it('groups fragments by source and orders by seq', () => {
    const session = store.session('s4');
    session.ingest(makeEvent('srcB', 2, 'final', 'B2'));
    session.ingest(makeEvent('srcA', 1, 'final', 'A1'));
    session.ingest(makeEvent('srcA', 2, 'partial', 'A2'));
    const snap = session.getSnapshot();
    expect(snap.sources).toHaveLength(2);
    expect(snap.sources[0].sourceId).toBe('srcA');
    expect(snap.sources[0].fragments.map((f) => f.sourceSeq)).toEqual([1, 2]);
    expect(snap.sources[1].sourceId).toBe('srcB');
    expect(snap.text).toBe('A1A2B2');
  });
});
