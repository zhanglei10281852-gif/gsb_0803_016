import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdtempSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { runWorker } from './spawn-helper';
import { forceRemove } from '../cleanup';

describe('e2e: session archive export/import', () => {
  let dir: string;

  beforeAll(() => {
    dir = mkdtempSync(join(tmpdir(), 'asr-e2e-archive-'));
  });
  afterAll(() => {
    forceRemove(dir);
  });

  const SOURCE_DB = () => join(dir, 'source.db');
  const TARGET_DB = () => join(dir, 'target.db');
  const ARCHIVE = () => join(dir, 'session.asra');

  async function seedSourceDatabase(dbPath: string): Promise<void> {
    await runWorker({
      action: 'ingest',
      dbPath,
      sessionId: 'archive-session',
      events: [
        {
          eventId: 'e1',
          sourceId: 'srcA',
          sourceSeq: 1,
          type: 'final',
          content: 'helo ',
        },
        {
          eventId: 'e2',
          sourceId: 'srcA',
          sourceSeq: 2,
          type: 'final',
          content: 'world',
        },
        {
          eventId: 'e3',
          sourceId: 'srcB',
          sourceSeq: 1,
          type: 'final',
          content: '!',
        },
      ],
    });

    const leaseRes = await runWorker({
      action: 'acquire-lease',
      dbPath,
      sessionId: 'archive-session',
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-alice',
      baseRevision: 2,
      ttlMs: 60000,
    });
    const lease = leaseRes.lease as { leaseId: string };

    await runWorker({
      action: 'submit-correction',
      dbPath,
      sessionId: 'archive-session',
      leaseId: lease.leaseId,
      correctedContent: 'Hello ',
      reason: 'capitalization',
    });

    await runWorker({
      action: 'consumer-ack',
      dbPath,
      sessionId: 'archive-session',
      consumerId: 'qc-isolated',
      revision: 3,
    });
  }

  it(
    'exports from one process and imports into an empty database',
    async () => {
      const sourceDb = SOURCE_DB();
      const targetDb = TARGET_DB();
      const archivePath = ARCHIVE();

      await seedSourceDatabase(sourceDb);

      const exportRes = await runWorker({
        action: 'export-archive',
        dbPath: sourceDb,
        sessionId: 'archive-session',
        archivePath,
      });
      expect(exportRes.ok).toBe(true);
      expect(exportRes.result.counts.revisions).toBeGreaterThan(0);
      expect(exportRes.result.counts.corrections).toBe(1);
      expect(exportRes.result.counts.cursors).toBe(1);

      const importRes = await runWorker({
        action: 'import-archive',
        dbPath: targetDb,
        archivePath,
      });
      expect(importRes.ok).toBe(true);
      expect(importRes.result.idempotent).toBe(false);

      const srcSnap = await runWorker({
        action: 'snapshot',
        dbPath: sourceDb,
        sessionId: 'archive-session',
      });
      const dstSnap = await runWorker({
        action: 'snapshot',
        dbPath: targetDb,
        sessionId: 'archive-session',
      });

      expect(dstSnap.snapshot.text).toBe(srcSnap.snapshot.text);
      expect(dstSnap.snapshot.text).toBe('Hello world!');
      expect(dstSnap.snapshot.revision).toBe(srcSnap.snapshot.revision);

      const correctionsRes = await runWorker({
        action: 'get-corrections',
        dbPath: targetDb,
        sessionId: 'archive-session',
        sourceId: 'srcA',
        sourceSeq: 1,
      });
      expect(correctionsRes.corrections).toHaveLength(1);
      expect(correctionsRes.corrections[0].actor).toBe('reviewer-alice');
      expect(correctionsRes.corrections[0].reason).toBe('capitalization');
      expect(correctionsRes.corrections[0].originalContent).toBe('helo ');
      expect(correctionsRes.corrections[0].supersedesCorrectionId).toBeNull();

      const consumerRes = await runWorker({
        action: 'consumer-read',
        dbPath: targetDb,
        sessionId: 'archive-session',
        consumerId: 'qc-isolated',
      });
      expect(consumerRes.cursor).toBe(3);
    },
    30000,
  );

  it(
    're-importing the same archive is idempotent',
    async () => {
      const targetDb = join(dir, 'idem-target.db');
      const archivePath = ARCHIVE();

      const r1 = await runWorker({
        action: 'import-archive',
        dbPath: targetDb,
        archivePath,
      });
      expect(r1.ok).toBe(true);
      expect(r1.result.idempotent).toBe(false);

      const r2 = await runWorker({
        action: 'import-archive',
        dbPath: targetDb,
        archivePath,
      });
      expect(r2.ok).toBe(true);
      expect(r2.result.idempotent).toBe(true);

      const r3 = await runWorker({
        action: 'import-archive',
        dbPath: targetDb,
        archivePath,
      });
      expect(r3.ok).toBe(true);
      expect(r3.result.idempotent).toBe(true);
    },
    30000,
  );

  it(
    'rejects a tampered archive and leaves no state',
    async () => {
      const targetDb = join(dir, 'tamper-target.db');
      const archivePath = join(dir, 'tampered.asra');

      const archiveBuf = readFileSync(ARCHIVE());
      const tampered = Buffer.from(archiveBuf);
      const idx = tampered.indexOf(Buffer.from('Hello'));
      expect(idx).toBeGreaterThan(-1);
      tampered[idx] = tampered[idx] ^ 0xff;
      writeFileSync(archivePath, tampered);

      const importRes = await runWorker({
        action: 'import-archive',
        dbPath: targetDb,
        archivePath,
      });
      expect(importRes.ok).toBe(false);
      expect(importRes.code).toBe('ARCHIVE_CHECKSUM');

      const existsRes = await runWorker({
        action: 'session-exists',
        dbPath: targetDb,
        sessionId: 'archive-session',
      });
      expect(existsRes.exists).toBe(false);
    },
    30000,
  );

  it(
    'rejects a truncated archive and leaves no state',
    async () => {
      const targetDb = join(dir, 'trunc-target.db');
      const archivePath = join(dir, 'truncated.asra');

      const archiveBuf = readFileSync(ARCHIVE());
      writeFileSync(archivePath, archiveBuf.subarray(0, archiveBuf.length - 50));

      const importRes = await runWorker({
        action: 'import-archive',
        dbPath: targetDb,
        archivePath,
      });
      expect(importRes.ok).toBe(false);
      expect(
        importRes.code === 'ARCHIVE_FORMAT' ||
          importRes.code === 'ARCHIVE_CHECKSUM',
      ).toBe(true);

      const existsRes = await runWorker({
        action: 'session-exists',
        dbPath: targetDb,
        sessionId: 'archive-session',
      });
      expect(existsRes.exists).toBe(false);
    },
    30000,
  );

  it(
    'crash mid-import leaves no partial state (atomic rollback)',
    async () => {
      const targetDb = join(dir, 'crash-target.db');
      const archivePath = ARCHIVE();

      const crashRes = await runWorker({
        action: 'import-archive-crash-midway',
        dbPath: targetDb,
        archivePath,
      });
      expect(crashRes.killed).toBe(true);

      const existsRes = await runWorker({
        action: 'session-exists',
        dbPath: targetDb,
        sessionId: 'archive-session',
      });
      expect(existsRes.exists).toBe(false);
    },
    30000,
  );

  it(
    're-import succeeds after a failed attempt',
    async () => {
      const targetDb = join(dir, 'retry-target.db');
      const archivePath = ARCHIVE();

      const badArchive = join(dir, 'bad.asra');
      const buf = readFileSync(archivePath);
      writeFileSync(badArchive, buf.subarray(0, buf.length - 40));

      const failRes = await runWorker({
        action: 'import-archive',
        dbPath: targetDb,
        archivePath: badArchive,
      });
      expect(failRes.ok).toBe(false);

      const successRes = await runWorker({
        action: 'import-archive',
        dbPath: targetDb,
        archivePath,
      });
      expect(successRes.ok).toBe(true);
      expect(successRes.result.idempotent).toBe(false);
      expect(successRes.snapshot.text).toBe('Hello world!');

      const idempotentRes = await runWorker({
        action: 'import-archive',
        dbPath: targetDb,
        archivePath,
      });
      expect(idempotentRes.result.idempotent).toBe(true);
    },
    30000,
  );

  it(
    'consumer resumes from correct cursor after cross-environment import',
    async () => {
      const targetDb = join(dir, 'cursor-target.db');
      const archivePath = ARCHIVE();

      await runWorker({
        action: 'import-archive',
        dbPath: targetDb,
        archivePath,
      });

      const existingConsumer = await runWorker({
        action: 'consumer-read',
        dbPath: targetDb,
        sessionId: 'archive-session',
        consumerId: 'qc-isolated',
      });
      expect(existingConsumer.cursor).toBe(3);

      const newConsumer = await runWorker({
        action: 'consumer-read',
        dbPath: targetDb,
        sessionId: 'archive-session',
        consumerId: 'new-consumer',
      });
      expect(newConsumer.cursor).toBe(0);
      const revisions = newConsumer.revisions as Array<{
        revision: number;
        changeType: string;
      }>;
      expect(revisions.length).toBeGreaterThan(0);
      expect(revisions[0].revision).toBe(1);
      const hasCorrection = revisions.some(
        (r) => r.changeType === 'correction-applied',
      );
      expect(hasCorrection).toBe(true);
    },
    30000,
  );
});
