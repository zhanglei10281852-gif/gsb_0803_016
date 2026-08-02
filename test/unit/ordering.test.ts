import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTempStore, makeEvent, shuffle } from '../helpers';
import { RecognitionStore, SlotFinalizedError } from '../../src';

describe('ordering and final protection', () => {
  let store: RecognitionStore;
  let cleanup: () => void;

  beforeEach(() => {
    const ctx = createTempStore();
    store = ctx.store;
    cleanup = ctx.cleanup;
  });
  afterEach(() => cleanup());

  it('older partial cannot overwrite newer partial', () => {
    const session = store.session('s1');
    session.ingest(makeEvent('srcA', 3, 'partial', 'newer'));
    const result = session.ingest(makeEvent('srcA', 1, 'partial', 'older'));
    expect(result.status).toBe('ignored');
    const snap = session.getSnapshot();
    expect(snap.text).toBe('newer');
  });

  it('late partial cannot overwrite final', () => {
    const session = store.session('s2');
    session.ingest(makeEvent('srcA', 1, 'final', 'final text'));
    const result = session.ingest(
      makeEvent('srcA', 1, 'partial', 'late partial'),
    );
    expect(result.status).toBe('ignored');
    expect(result.reason).toContain('finalized');
    const snap = session.getSnapshot();
    expect(snap.text).toBe('final text');
  });

  it('final replaces current partial', () => {
    const session = store.session('s3');
    session.ingest(makeEvent('srcA', 1, 'partial', 'partial text'));
    const result = session.ingest(makeEvent('srcA', 1, 'final', 'final text'));
    expect(result.status).toBe('accepted');
    const snap = session.getSnapshot();
    expect(snap.text).toBe('final text');
    expect(snap.sources[0].fragments[0].type).toBe('final');
  });

  it('rejects double finalization of same slot with different eventId', () => {
    const session = store.session('s4');
    session.ingest(makeEvent('srcA', 1, 'final', 'first', 'e1'));
    expect(() =>
      session.ingest(makeEvent('srcA', 1, 'final', 'second', 'e2')),
    ).toThrow(SlotFinalizedError);
  });

  it('out-of-order finals produce deterministic snapshot by sourceSeq', () => {
    const session = store.session('s5');
    const events = [
      makeEvent('srcA', 3, 'final', 'C'),
      makeEvent('srcA', 1, 'final', 'A'),
      makeEvent('srcA', 2, 'final', 'B'),
    ];
    for (const e of shuffle(events)) {
      session.ingest(e);
    }
    const snap = session.getSnapshot();
    expect(snap.text).toBe('ABC');
  });

  it('partial for future seq coexists with older final', () => {
    const session = store.session('s6');
    session.ingest(makeEvent('srcA', 1, 'final', 'done'));
    session.ingest(makeEvent('srcA', 2, 'partial', 'next'));
    const snap = session.getSnapshot();
    expect(snap.text).toBe('donenext');
    expect(snap.sources[0].fragments).toHaveLength(2);
  });

  it('multiple sources ordered lexicographically by sourceId', () => {
    const session = store.session('s7');
    session.ingest(makeEvent('srcB', 1, 'final', 'B'));
    session.ingest(makeEvent('srcA', 1, 'final', 'A'));
    session.ingest(makeEvent('srcC', 1, 'final', 'C'));
    const snap = session.getSnapshot();
    expect(snap.text).toBe('ABC');
    expect(snap.sources.map((s) => s.sourceId)).toEqual([
      'srcA',
      'srcB',
      'srcC',
    ]);
  });

  it('same event set in any order produces identical final text and summary', () => {
    const events = [
      makeEvent('srcA', 1, 'partial', 'hello', 'a1p'),
      makeEvent('srcA', 1, 'final', 'hello ', 'a1f'),
      makeEvent('srcA', 2, 'partial', 'wor', 'a2p'),
      makeEvent('srcA', 2, 'final', 'world', 'a2f'),
      makeEvent('srcB', 1, 'partial', 'foo', 'b1p'),
      makeEvent('srcB', 1, 'final', 'foo!', 'b1f'),
    ];

    const runOnce = (order: typeof events) => {
      const ctx = createTempStore();
      try {
        const s = ctx.store.session('det');
        for (const e of order) s.ingest({ ...e });
        const snap = s.getSnapshot();
        return { text: snap.text, summary: snap.summary };
      } finally {
        ctx.cleanup();
      }
    };

    const baseline = runOnce(events);
    for (let i = 0; i < 5; i++) {
      const result = runOnce(shuffle(events));
      expect(result.text).toBe(baseline.text);
      expect(result.summary).toEqual(baseline.summary);
    }
  });

  it('unchanged partial content does not create revision', () => {
    const session = store.session('s8');
    session.ingest(makeEvent('srcA', 1, 'partial', 'same', 'e1'));
    const r = session.ingest(makeEvent('srcA', 1, 'partial', 'same', 'e2'));
    expect(r.status).toBe('ignored');
    expect(session.getLatestRevisionNumber()).toBe(1);
  });
});
