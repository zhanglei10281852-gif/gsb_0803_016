import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTempStore, makeEvent } from '../helpers';
import { RecognitionStore } from '../../src';

describe('consumer', () => {
  let store: RecognitionStore;
  let cleanup: () => void;

  beforeEach(() => {
    const ctx = createTempStore();
    store = ctx.store;
    cleanup = ctx.cleanup;
  });
  afterEach(() => cleanup());

  it('starts at cursor 0', () => {
    const consumer = store.consumer('s1', 'c1');
    expect(consumer.getCursor()).toBe(0);
    expect(consumer.read()).toEqual([]);
  });

  it('reads new revisions after cursor', () => {
    const session = store.session('s2');
    session.ingest(makeEvent('srcA', 1, 'final', 'a', 'e1'));
    session.ingest(makeEvent('srcA', 2, 'final', 'b', 'e2'));
    const consumer = store.consumer('s2', 'c1');
    const batch = consumer.read();
    expect(batch).toHaveLength(2);
    expect(batch.map((r) => r.revision)).toEqual([1, 2]);
  });

  it('ack advances cursor and prevents redelivery', () => {
    const session = store.session('s3');
    session.ingest(makeEvent('srcA', 1, 'final', 'a', 'e1'));
    session.ingest(makeEvent('srcA', 2, 'final', 'b', 'e2'));
    const consumer = store.consumer('s3', 'c1');

    const batch1 = consumer.read();
    expect(batch1).toHaveLength(2);
    consumer.ack(batch1[batch1.length - 1].revision);
    expect(consumer.getCursor()).toBe(2);

    const batch2 = consumer.read();
    expect(batch2).toEqual([]);
  });

  it('does not redeliver acknowledged revisions', () => {
    const session = store.session('s4');
    session.ingest(makeEvent('srcA', 1, 'final', 'a', 'e1'));
    const consumer = store.consumer('s4', 'c1');
    consumer.ack(1);
    session.ingest(makeEvent('srcA', 2, 'final', 'b', 'e2'));
    const batch = consumer.read();
    expect(batch).toHaveLength(1);
    expect(batch[0].revision).toBe(2);
  });

  it('different consumers have independent cursors', () => {
    const session = store.session('s5');
    session.ingest(makeEvent('srcA', 1, 'final', 'a', 'e1'));
    session.ingest(makeEvent('srcA', 2, 'final', 'b', 'e2'));

    const c1 = store.consumer('s5', 'consumer-1');
    const c2 = store.consumer('s5', 'consumer-2');

    c1.ack(1);
    expect(c1.read()).toHaveLength(1);
    expect(c2.read()).toHaveLength(2);
  });

  it('cursor persists across reopen', () => {
    const ctx = createTempStore();
    try {
      const s = ctx.store.session('persist');
      s.ingest(makeEvent('srcA', 1, 'final', 'a', 'e1'));
      s.ingest(makeEvent('srcA', 2, 'final', 'b', 'e2'));
      const c1 = ctx.store.consumer('persist', 'c1');
      c1.ack(1);
      ctx.store.close();

      const store2 = RecognitionStore.open({ dbPath: ctx.dbPath });
      try {
        const c2 = store2.consumer('persist', 'c1');
        expect(c2.getCursor()).toBe(1);
        const batch = c2.read();
        expect(batch).toHaveLength(1);
        expect(batch[0].revision).toBe(2);
      } finally {
        store2.close();
      }
    } finally {
      ctx.cleanup();
    }
  });

  it('ack never moves cursor backward', () => {
    const session = store.session('s6');
    session.ingest(makeEvent('srcA', 1, 'final', 'a', 'e1'));
    session.ingest(makeEvent('srcA', 2, 'final', 'b', 'e2'));
    const consumer = store.consumer('s6', 'c1');
    consumer.ack(2);
    consumer.ack(1);
    expect(consumer.getCursor()).toBe(2);
  });

  it('read respects batch limit', () => {
    const session = store.session('s7');
    for (let i = 0; i < 5; i++) {
      session.ingest(makeEvent('srcA', i, 'final', `t${i}`, `e${i}`));
    }
    const consumer = store.consumer('s7', 'c1');
    const batch = consumer.read(2);
    expect(batch).toHaveLength(2);
    expect(batch.map((r) => r.revision)).toEqual([1, 2]);
  });
});
