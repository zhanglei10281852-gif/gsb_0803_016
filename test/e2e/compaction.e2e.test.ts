import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { runWorker } from './spawn-helper';
import { forceRemove } from '../cleanup';

describe('e2e: compaction across processes', () => {
  let dir: string;

  beforeAll(() => {
    dir = mkdtempSync(join(tmpdir(), 'asr-e2e-compact-'));
  });
  afterAll(() => {
    forceRemove(dir);
  });

  const DB = () => join(dir, 'compact.db');
  const ARCHIVE = () => join(dir, 'checkpoint.asra');

  async function seedDatabase(): Promise<void> {
    const events = [];
    for (let i = 0; i < 20; i++) {
      events.push({
        eventId: `e${i}`,
        sourceId: 'srcA',
        sourceSeq: i,
        type: 'final',
        content: `seg-${i}-`,
      });
    }
    await runWorker({
      action: 'ingest',
      dbPath: DB(),
      sessionId: 'cs',
      events,
    });
  }

  it(
    'compacts after archive, snapshot unchanged, expired consumer gets reset-required',
    async () => {
      await seedDatabase();

      const exportRes = await runWorker({
        action: 'export-archive',
        dbPath: DB(),
        sessionId: 'cs',
        archivePath: ARCHIVE(),
      });
      expect(exportRes.ok).toBe(true);

      await runWorker({
        action: 'consumer-ack',
        dbPath: DB(),
        sessionId: 'cs',
        consumerId: 'slow-reader',
        revision: 5,
      });

      const compactRes = await runWorker({
        action: 'compact-session',
        dbPath: DB(),
        sessionId: 'cs',
      });
      expect(compactRes.ok).toBe(true);
      const stats = compactRes.stats;
      expect(stats.compacted).toBe(true);
      expect(stats.safeRevision).toBe(5);
      expect(stats.revisionsRemoved).toBe(5);
      expect(stats.revisionsRemaining).toBe(15);

      const slowRead = await runWorker({
        action: 'consumer-read',
        dbPath: DB(),
        sessionId: 'cs',
        consumerId: 'slow-reader',
      });
      expect(slowRead.ok).toBe(true);
      expect(slowRead.resetRequired).toBe(false);
      expect(slowRead.revisions[0].revision).toBe(6);

      const expiredRead = await runWorker({
        action: 'consumer-read',
        dbPath: DB(),
        sessionId: 'cs',
        consumerId: 'very-late',
      });
      expect(expiredRead.ok).toBe(false);
      expect(expiredRead.code).toBe('CONSUMER_RESET_REQUIRED');
      expect(expiredRead.resetRequired).toBe(true);
      expect(expiredRead.safeRevision).toBe(5);

      const snapRes = await runWorker({
        action: 'snapshot',
        dbPath: DB(),
        sessionId: 'cs',
      });
      const text = snapRes.snapshot.text;
      expect(text).toContain('seg-19-');
      expect(text.startsWith('seg-0-')).toBe(true);
    },
    30000,
  );

  it(
    'concurrent compact and writes maintain consistent revision ordering',
    async () => {
      const dbPath = join(dir, 'concurrent.db');
      const archivePath = join(dir, 'concurrent.asra');

      const initial = [];
      for (let i = 0; i < 10; i++) {
        initial.push({
          eventId: `ie${i}`,
          sourceId: 'srcA',
          sourceSeq: i,
          type: 'final',
          content: `i${i}-`,
        });
      }
      await runWorker({
        action: 'ingest',
        dbPath,
        sessionId: 'cc',
        events: initial,
      });
      await runWorker({
        action: 'export-archive',
        dbPath,
        sessionId: 'cc',
        archivePath,
      });

      const writes: Promise<unknown>[] = [];
      for (let i = 10; i < 20; i++) {
        writes.push(
          runWorker({
            action: 'ingest',
            dbPath,
            sessionId: 'cc',
            events: [
              {
                eventId: `we${i}`,
                sourceId: 'srcA',
                sourceSeq: i,
                type: 'final',
                content: `w${i}-`,
              },
            ],
          }),
        );
      }
      const compact = runWorker({
        action: 'compact-session',
        dbPath,
        sessionId: 'cc',
      });

      const [, compactRes] = await Promise.all([
        Promise.all(writes),
        compact,
      ]);
      expect(compactRes.ok).toBe(true);

      const snapRes = await runWorker({
        action: 'snapshot',
        dbPath,
        sessionId: 'cc',
      });
      expect(snapRes.snapshot.text).toContain('w19-');

      const revRes = await runWorker({
        action: 'revisions-count',
        dbPath,
        sessionId: 'cc',
      });
      expect(revRes.count).toBeGreaterThan(0);
    },
    30000,
  );

  it(
    'post-compaction archive verifies and imports with identical snapshot',
    async () => {
      const postArchive = join(dir, 'post-compact.asra');
      const targetDb = join(dir, 'post-compact-import.db');

      const exportRes = await runWorker({
        action: 'export-archive',
        dbPath: DB(),
        sessionId: 'cs',
        archivePath: postArchive,
      });
      expect(exportRes.ok).toBe(true);

      const importRes = await runWorker({
        action: 'import-archive',
        dbPath: targetDb,
        archivePath: postArchive,
      });
      expect(importRes.ok).toBe(true);
      expect(importRes.result.idempotent).toBe(false);

      const srcSnap = await runWorker({
        action: 'snapshot',
        dbPath: DB(),
        sessionId: 'cs',
      });
      const dstSnap = await runWorker({
        action: 'snapshot',
        dbPath: targetDb,
        sessionId: 'cs',
      });

      expect(dstSnap.snapshot.text).toBe(srcSnap.snapshot.text);
      expect(dstSnap.snapshot.revision).toBe(srcSnap.snapshot.revision);

      const secondImport = await runWorker({
        action: 'import-archive',
        dbPath: targetDb,
        archivePath: postArchive,
      });
      expect(secondImport.result.idempotent).toBe(true);
    },
    30000,
  );

  it(
    'consumer recovers from reset by acking to watermark and continuing',
    async () => {
      const recoverDb = join(dir, 'recover.db');
      const recoverArchive = join(dir, 'recover.asra');

      const events = [];
      for (let i = 0; i < 10; i++) {
        events.push({
          eventId: `r${i}`,
          sourceId: 'srcA',
          sourceSeq: i,
          type: 'final',
          content: `r${i}-`,
        });
      }
      await runWorker({
        action: 'ingest',
        dbPath: recoverDb,
        sessionId: 'rc',
        events,
      });
      await runWorker({
        action: 'export-archive',
        dbPath: recoverDb,
        sessionId: 'rc',
        archivePath: recoverArchive,
      });
      await runWorker({
        action: 'compact-session',
        dbPath: recoverDb,
        sessionId: 'rc',
      });

      const resetRead = await runWorker({
        action: 'consumer-read',
        dbPath: recoverDb,
        sessionId: 'rc',
        consumerId: 'newbie',
      });
      expect(resetRead.code).toBe('CONSUMER_RESET_REQUIRED');
      expect(resetRead.safeRevision).toBe(10);

      await runWorker({
        action: 'consumer-ack',
        dbPath: recoverDb,
        sessionId: 'rc',
        consumerId: 'newbie',
        revision: resetRead.safeRevision,
      });

      const afterAck = await runWorker({
        action: 'consumer-read',
        dbPath: recoverDb,
        sessionId: 'rc',
        consumerId: 'newbie',
      });
      expect(afterAck.ok).toBe(true);
      expect(afterAck.resetRequired).toBe(false);
      expect(afterAck.revisions).toHaveLength(0);
    },
    30000,
  );
});
