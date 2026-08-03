import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createTempStore, makeEvent } from '../helpers';
import { forceRemove } from '../cleanup';
import {
  RecognitionStore,
  ConsumerResetRequiredError,
} from '../../src';

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

describe('compaction', () => {
  let store: RecognitionStore;
  let cleanup: () => void;
  let archiveDir: string;

  beforeEach(async () => {
    const ctx = createTempStore();
    store = ctx.store;
    cleanup = ctx.cleanup;
    archiveDir = mkdtempSync(join(tmpdir(), 'asr-compact-'));

    const session = store.session('compact');
    for (let i = 0; i < 10; i++) {
      session.ingest(
        makeEvent('srcA', i, 'final', `chunk-${i}-`, `e${i}`),
      );
    }
    await store.exportSession('compact', join(archiveDir, 'a1.asra'));
  });

  afterEach(() => {
    cleanup();
    forceRemove(archiveDir);
  });

  it('does not compact without an archive checkpoint', () => {
    const fresh = createTempStore();
    try {
      const s = fresh.store.session('no-archive');
      s.ingest(makeEvent('srcA', 0, 'final', 'x', 'e1'));
      const stats = fresh.store.compactSession('no-archive');
      expect(stats.compacted).toBe(false);
      expect(stats.skippedReason).toContain('checkpoint');
    } finally {
      fresh.cleanup();
    }
  });

  it('compacts revisions before safe watermark with no active consumers', () => {
    const stats = store.compactSession('compact');
    expect(stats.compacted).toBe(true);
    expect(stats.safeRevision).toBe(10);
    expect(stats.slowestActiveCursor).toBeNull();
    expect(stats.revisionsRemoved).toBe(10);
    expect(stats.revisionsRemaining).toBe(0);
    expect(stats.eventsRemoved).toBe(10);

    const session = store.session('compact');
    expect(session.getLatestRevisionNumber()).toBe(10);
    const snap = session.getSnapshot();
    expect(snap.text).toBe(
      'chunk-0-chunk-1-chunk-2-chunk-3-chunk-4-chunk-5-chunk-6-chunk-7-chunk-8-chunk-9-',
    );
  });

  it('preserves current snapshot after compaction', () => {
    const before = store.session('compact').getSnapshot();
    store.compactSession('compact');
    const after = store.session('compact').getSnapshot();
    expect(after.text).toBe(before.text);
    expect(after.revision).toBe(before.revision);
    expect(after.summary).toEqual(before.summary);
  });

  it('does not compact beyond the slowest active consumer cursor', () => {
    const fast = store.consumer('compact', 'fast', { leaseTtlMs: 60000 });
    const slow = store.consumer('compact', 'slow', { leaseTtlMs: 60000 });

    fast.ack(8);
    slow.ack(3);

    const stats = store.compactSession('compact');
    expect(stats.compacted).toBe(true);
    expect(stats.slowestActiveCursor).toBe(3);
    expect(stats.safeRevision).toBe(3);
    expect(stats.revisionsRemoved).toBe(3);
    expect(stats.revisionsRemaining).toBe(7);

    const fastBatch = fast.read(100);
    expect(fastBatch).toHaveLength(2);
    expect(fastBatch[0].revision).toBe(9);

    const slowBatch = slow.read(100);
    expect(slowBatch).toHaveLength(7);
    expect(slowBatch[0].revision).toBe(4);
  });

  it('expired consumer lease does not block compaction', async () => {
    const short = store.consumer('compact', 'short-lived', {
      leaseTtlMs: 50,
    });
    short.ack(3);
    await sleep(80);

    const stats = store.compactSession('compact');
    expect(stats.compacted).toBe(true);
    expect(stats.slowestActiveCursor).toBeNull();
    expect(stats.safeRevision).toBe(10);
    expect(stats.revisionsRemoved).toBe(10);
  });

  it('heartbeat renews consumer lease and blocks compaction', async () => {
    const c = store.consumer('compact', 'hb', { leaseTtlMs: 80 });
    c.ack(5);
    await sleep(40);
    c.heartbeat();
    await sleep(50);

    const info = c.getLeaseInfo();
    expect(info.active).toBe(true);

    const stats = store.compactSession('compact');
    expect(stats.slowestActiveCursor).toBe(5);
    expect(stats.safeRevision).toBe(5);
  });

  it('consumer below watermark receives reset-required and can recover', () => {
    store.compactSession('compact');

    const consumer = store.consumer('compact', 'laggard', {
      leaseTtlMs: 60000,
    });
    consumer.ack(2);

    let caught: ConsumerResetRequiredError | null = null;
    try {
      consumer.read(100);
    } catch (err) {
      caught = err as ConsumerResetRequiredError;
    }
    expect(caught).not.toBeNull();
    expect(caught!.code).toBe('CONSUMER_RESET_REQUIRED');
    expect(caught!.safeRevision).toBe(10);

    const snap = consumer.getSnapshot();
    expect(snap.text).toContain('chunk-9');
    expect(snap.revision).toBe(10);

    consumer.ack(10);
    expect(consumer.read(100)).toEqual([]);
  });

  it('consumer at or above watermark reads normally after compaction', () => {
    const consumer = store.consumer('compact', 'caught-up', {
      leaseTtlMs: 60000,
    });
    consumer.ack(10);
    store.compactSession('compact');
    expect(consumer.read(100)).toEqual([]);
  });

  it('new writes after compaction create revisions above watermark', () => {
    store.compactSession('compact');

    const session = store.session('compact');
    session.ingest(makeEvent('srcA', 10, 'final', 'new-', 'e10'));

    const consumer = store.consumer('compact', 'new-reader', {
      leaseTtlMs: 60000,
    });
    expect(() => consumer.read(100)).toThrow(ConsumerResetRequiredError);

    const snap = consumer.getSnapshot();
    expect(snap.revision).toBe(11);

    consumer.ack(11);
    expect(consumer.read(100)).toEqual([]);
  });

  it('archive produced after compaction is verifiable and imports identically', async () => {
    const beforeSnap = store.session('compact').getSnapshot();
    store.compactSession('compact');

    const postArchive = join(archiveDir, 'post-compact.asra');
    const exportResult = await store.exportSession(
      'compact',
      postArchive,
    );
    expect(exportResult.counts.revisions).toBe(0);
    expect(exportResult.counts.checkpoints).toBeGreaterThanOrEqual(1);

    const importStore = RecognitionStore.open({
      dbPath: join(archiveDir, 'imported.db'),
    });
    try {
      const result = importStore.importSession(postArchive);
      expect(result.idempotent).toBe(false);

      const importedSnap = importStore.session('compact').getSnapshot();
      expect(importedSnap.text).toBe(beforeSnap.text);
      expect(importedSnap.revision).toBe(beforeSnap.revision);
    } finally {
      importStore.close();
    }
  });

  it('compaction is safe when more revisions accumulate before next archive', async () => {
    store.compactSession('compact');

    const session = store.session('compact');
    for (let i = 10; i < 15; i++) {
      session.ingest(makeEvent('srcA', i, 'final', `c${i}-`, `e${i}`));
    }

    const consumer = store.consumer('compact', 'reader', {
      leaseTtlMs: 60000,
    });
    expect(() => consumer.read(100)).toThrow(ConsumerResetRequiredError);
    consumer.ack(10);
    const batch = consumer.read(100);
    expect(batch).toHaveLength(5);
    consumer.ack(12);

    await store.exportSession('compact', join(archiveDir, 'a2.asra'));
    const stats = store.compactSession('compact');
    expect(stats.safeRevision).toBe(12);
    expect(stats.revisionsRemoved).toBe(2);
    expect(stats.revisionsRemaining).toBe(3);
  });

  it('releaseLease removes consumer from active set', () => {
    const consumer = store.consumer('compact', 'temp', {
      leaseTtlMs: 60000,
    });
    consumer.ack(5);
    expect(consumer.getLeaseInfo().active).toBe(true);

    consumer.releaseLease();
    expect(consumer.getLeaseInfo().active).toBe(false);

    const stats = store.compactSession('compact');
    expect(stats.slowestActiveCursor).toBeNull();
    expect(stats.safeRevision).toBe(10);
  });

  it('observable stats report compaction skipped when no checkpoint', () => {
    const fresh = createTempStore();
    try {
      const s = fresh.store.session('tiny');
      s.ingest(makeEvent('srcA', 0, 'final', 'x', 'e1'));
      const stats = fresh.store.compactSession('tiny');
      expect(stats.compacted).toBe(false);
      expect(stats.revisionsRemoved).toBe(0);
      expect(stats.skippedReason).toContain('checkpoint');
    } finally {
      fresh.cleanup();
    }
  });
});
