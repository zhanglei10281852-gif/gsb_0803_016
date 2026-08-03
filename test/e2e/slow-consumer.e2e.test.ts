import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTempStore, makeEvent } from '../helpers';
import { RecognitionStore } from '../../src';

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

describe('e2e: slow consumer', () => {
  let store: RecognitionStore;
  let cleanup: () => void;

  beforeEach(() => {
    const ctx = createTempStore();
    store = ctx.store;
    cleanup = ctx.cleanup;
  });
  afterEach(() => cleanup());

  it('slow consumer receives all revisions without gaps', async () => {
    const session = store.session('slow');
    const consumer = store.consumer('slow', 'slow-consumer');

    const totalRevisions = 50;
    for (let i = 0; i < totalRevisions; i++) {
      session.ingest(
        makeEvent('srcA', i, 'final', `chunk-${i}-`, `evt-${i}`),
      );
    }

    const received: number[] = [];
    while (received.length < totalRevisions) {
      const batch = consumer.read(5);
      if (batch.length === 0) break;
      for (const rev of batch) {
        received.push(rev.revision);
      }
      consumer.ack(batch[batch.length - 1].revision);
      await sleep(5);
    }

    expect(received).toHaveLength(totalRevisions);
    expect(received).toEqual(
      Array.from({ length: totalRevisions }, (_, i) => i + 1),
    );
  });

  it('acknowledged revisions are never redelivered', () => {
    const session = store.session('redeliver');
    const consumer = store.consumer('redeliver', 'c1');

    for (let i = 0; i < 10; i++) {
      session.ingest(makeEvent('srcA', i, 'final', `t${i}`, `e${i}`));
    }

    const first = consumer.read(3);
    expect(first.map((r) => r.revision)).toEqual([1, 2, 3]);
    consumer.ack(3);

    const second = consumer.read(3);
    expect(second.map((r) => r.revision)).toEqual([4, 5, 6]);
    consumer.ack(6);

    const third = consumer.read(100);
    expect(third.map((r) => r.revision)).toEqual([7, 8, 9, 10]);
    consumer.ack(10);

    const fourth = consumer.read(100);
    expect(fourth).toEqual([]);
  });

  it('unacknowledged revisions are not lost when new ones arrive', () => {
    const session = store.session('unacked');
    const consumer = store.consumer('unacked', 'c1');

    session.ingest(makeEvent('srcA', 0, 'final', 'a', 'e0'));
    const batch1 = consumer.read(10);
    expect(batch1).toHaveLength(1);

    session.ingest(makeEvent('srcA', 1, 'final', 'b', 'e1'));
    session.ingest(makeEvent('srcA', 2, 'final', 'c', 'e2'));

    const batch2 = consumer.read(10);
    expect(batch2.map((r) => r.revision)).toEqual([1, 2, 3]);
    consumer.ack(3);

    const batch3 = consumer.read(10);
    expect(batch3).toEqual([]);
  });

  it('multiple consumers at different positions independently track cursors', () => {
    const session = store.session('multi');
    for (let i = 0; i < 15; i++) {
      session.ingest(makeEvent('srcA', i, 'final', `t${i}`, `e${i}`));
    }

    const fast = store.consumer('multi', 'fast');
    const slow = store.consumer('multi', 'slow');

    fast.ack(10);
    slow.ack(5);

    expect(fast.read(100).map((r) => r.revision)).toEqual([
      11, 12, 13, 14, 15,
    ]);
    expect(slow.read(100).map((r) => r.revision)).toEqual([
      6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    ]);
  });

  it('stream async iterator delivers live revisions', async () => {
    const session = store.session('stream-test');
    const consumer = store.consumer('stream-test', 'streamer');
    const controller = new AbortController();

    const received: number[] = [];
    const consumePromise = (async () => {
      for await (const batch of consumer.stream({
        batchSize: 3,
        pollIntervalMs: 20,
        signal: controller.signal,
      })) {
        for (const rev of batch) {
          received.push(rev.revision);
        }
        consumer.ack(batch[batch.length - 1].revision);
        if (received.length >= 8) {
          controller.abort();
          break;
        }
      }
    })();

    for (let i = 0; i < 8; i++) {
      session.ingest(makeEvent('srcA', i, 'final', `s${i}`, `se${i}`));
      await sleep(30);
    }

    await consumePromise;
    expect(received).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(consumer.getCursor()).toBe(8);
  });

  it('consumer can resume after restarting from persisted cursor', async () => {
    const ctx = createTempStore();
    try {
      const s1 = ctx.store.session('resume');
      for (let i = 0; i < 20; i++) {
        s1.ingest(makeEvent('srcA', i, 'final', `r${i}`, `re${i}`));
      }
      const c1 = ctx.store.consumer('resume', 'resumer');
      const first = c1.read(10);
      c1.ack(first[first.length - 1].revision);
      expect(c1.getCursor()).toBe(10);
      ctx.store.close();

      const store2 = RecognitionStore.open({ dbPath: ctx.dbPath });
      try {
        const c2 = store2.consumer('resume', 'resumer');
        expect(c2.getCursor()).toBe(10);
        const rest = c2.read(100);
        expect(rest).toHaveLength(10);
        expect(rest[0].revision).toBe(11);
        expect(rest[9].revision).toBe(20);
        c2.ack(20);
        expect(c2.getCursor()).toBe(20);
      } finally {
        store2.close();
      }
    } finally {
      ctx.cleanup();
    }
  });
});
