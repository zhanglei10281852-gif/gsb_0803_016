import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTempStore, makeEvent } from '../helpers';
import { RecognitionStore } from '../../src';

describe('revision stream', () => {
  let store: RecognitionStore;
  let cleanup: () => void;

  beforeEach(() => {
    const ctx = createTempStore();
    store = ctx.store;
    cleanup = ctx.cleanup;
  });
  afterEach(() => cleanup());

  it('assigns strictly increasing revision numbers', () => {
    const session = store.session('s1');
    const revisions: number[] = [];
    for (let i = 0; i < 10; i++) {
      const r = session.ingest(
        makeEvent('srcA', i, 'partial', `text${i}`, `e${i}`),
      );
      expect(r.status).toBe('accepted');
      revisions.push(r.revision!);
    }
    expect(revisions).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });

  it('revision contains snapshot text at that point', () => {
    const session = store.session('s2');
    session.ingest(makeEvent('srcA', 1, 'partial', 'hello', 'e1'));
    const r2 = session.ingest(makeEvent('srcA', 1, 'final', 'hello!', 'e2'));
    const rev = session.getRevision(r2.revision!);
    expect(rev).not.toBeNull();
    expect(rev!.snapshotText).toBe('hello!');
    expect(rev!.changeType).toBe('final-committed');
  });

  it('getRevisionsSince returns all revisions after cursor', () => {
    const session = store.session('s3');
    for (let i = 0; i < 5; i++) {
      session.ingest(makeEvent('srcA', i, 'final', `t${i}`, `e${i}`));
    }
    const all = session.getRevisionsSince(0, 100);
    expect(all).toHaveLength(5);
    const after2 = session.getRevisionsSince(2, 100);
    expect(after2.map((r) => r.revision)).toEqual([3, 4, 5]);
  });

  it('getRevisionsSince respects limit', () => {
    const session = store.session('s4');
    for (let i = 0; i < 10; i++) {
      session.ingest(makeEvent('srcA', i, 'final', `t${i}`, `e${i}`));
    }
    const batch = session.getRevisionsSince(0, 3);
    expect(batch).toHaveLength(3);
    expect(batch.map((r) => r.revision)).toEqual([1, 2, 3]);
  });

  it('revisions persist after reopen', () => {
    const ctx = createTempStore();
    try {
      const s1 = ctx.store.session('persist');
      s1.ingest(makeEvent('srcA', 1, 'final', 'persisted', 'e1'));
      s1.ingest(makeEvent('srcA', 2, 'final', 'text', 'e2'));
      ctx.store.close();

      const store2 = RecognitionStore.open({ dbPath: ctx.dbPath });
      try {
        const s2 = store2.session('persist');
        expect(s2.getLatestRevisionNumber()).toBe(2);
        const revs = s2.getRevisionsSince(0, 100);
        expect(revs).toHaveLength(2);
        expect(s2.getSnapshot().text).toBe('persistedtext');
      } finally {
        store2.close();
      }
    } finally {
      ctx.cleanup();
    }
  });

  it('revision summary is deterministic', () => {
    const session = store.session('s6');
    session.ingest(makeEvent('srcA', 1, 'final', 'abc', 'e1'));
    const r = session.ingest(makeEvent('srcB', 1, 'partial', 'def', 'e2'));
    const rev = session.getRevision(r.revision!);
    expect(rev!.summary).toEqual({
      sourceCount: 2,
      fragmentCount: 2,
      finalCount: 1,
      partialCount: 1,
      correctedCount: 0,
      textLength: 6,
    });
  });
});
