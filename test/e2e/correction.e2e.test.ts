import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { runWorker } from './spawn-helper';
import { forceRemove } from '../cleanup';
import { RecognitionStore } from '../../src';

const SESSION_ID = 'correction-e2e';

describe('e2e: correction lease workflow across processes', () => {
  let dbPath: string;
  let dir: string;

  beforeAll(() => {
    dir = mkdtempSync(join(tmpdir(), 'asr-e2e-corr-'));
    dbPath = join(dir, 'correction.db');
  });

  afterAll(() => {
    forceRemove(dir);
  });

  it(
    'only one of multiple concurrent reviewers acquires the lease',
    async () => {
      await runWorker({
        action: 'ingest',
        dbPath,
        sessionId: SESSION_ID,
        events: [
          {
            eventId: 'e1',
            sourceId: 'srcA',
            sourceSeq: 1,
            type: 'final',
            content: 'helo world',
          },
        ],
      });

      const attempts = await Promise.all(
        ['alice', 'bob', 'carol'].map((actor) =>
          runWorker({
            action: 'acquire-lease',
            dbPath,
            sessionId: SESSION_ID,
            sourceId: 'srcA',
            sourceSeq: 1,
            actor,
            baseRevision: 1,
            ttlMs: 60000,
          }),
        ),
      );

      const successes = attempts.filter((r) => r.ok);
      const failures = attempts.filter((r) => !r.ok);
      expect(successes).toHaveLength(1);
      expect(failures).toHaveLength(2);
      for (const f of failures) {
        expect(f.code).toBe('LEASE_BUSY');
      }

      const lease = successes[0].lease as { leaseId: string; actor: string };
      expect(lease.actor).toMatch(/^(alice|bob|carol)$/);
    },
    30000,
  );

  it(
    'correction submits, persists across restart, and appears in consumer stream',
    async () => {
      const leaseRes = await runWorker({
        action: 'get-lease',
        dbPath,
        sessionId: SESSION_ID,
        sourceId: 'srcA',
        sourceSeq: 1,
      });
      expect(leaseRes.ok).toBe(true);
      const lease = leaseRes.lease as { leaseId: string };

      const corrRes = await runWorker({
        action: 'submit-correction',
        dbPath,
        sessionId: SESSION_ID,
        leaseId: lease.leaseId,
        correctedContent: 'hello world',
        reason: 'fixed typo helo->hello',
      });
      expect(corrRes.ok).toBe(true);
      const correction = corrRes.correction as {
        correctionId: string;
        revision: number;
        originalContent: string;
        correctedContent: string;
      };
      expect(correction.originalContent).toBe('helo world');
      expect(correction.correctedContent).toBe('hello world');

      const snapRes = await runWorker({
        action: 'snapshot',
        dbPath,
        sessionId: SESSION_ID,
      });
      const snap = snapRes.snapshot as { text: string; revision: number };
      expect(snap.text).toBe('hello world');

      const correctionsRes = await runWorker({
        action: 'get-corrections',
        dbPath,
        sessionId: SESSION_ID,
        sourceId: 'srcA',
        sourceSeq: 1,
      });
      const corrections = correctionsRes.corrections as Array<{
        correctionId: string;
        supersedesCorrectionId: string | null;
        actor: string;
      }>;
      expect(corrections).toHaveLength(1);
      expect(corrections[0].supersedesCorrectionId).toBeNull();

      const store2 = RecognitionStore.open({ dbPath });
      try {
        const s2 = store2.session(SESSION_ID);
        const snap2 = s2.getSnapshot();
        expect(snap2.text).toBe('hello world');
        expect(snap2.sources[0].fragments[0].corrected).toBe(true);

        const persistedCorrections = s2.getCorrectionsForFragment('srcA', 1);
        expect(persistedCorrections).toHaveLength(1);
        expect(persistedCorrections[0].correctionId).toBe(
          correction.correctionId,
        );
        expect(persistedCorrections[0].reason).toBe('fixed typo helo->hello');

        const persistedLease = s2.getLease('srcA', 1);
        expect(persistedLease).not.toBeNull();
        expect(persistedLease!.status).toBe('consumed');

        const consumer = store2.consumer(SESSION_ID, 'qc-engine');
        const batch = consumer.read(100);
        expect(batch).toHaveLength(2);
        expect(batch[0].changeType).toBe('final-committed');
        expect(batch[1].changeType).toBe('correction-applied');
        expect(batch[1].correctionId).toBe(correction.correctionId);
        expect(batch[1].correction).not.toBeNull();
        expect(batch[1].correction!.actor).toBeTruthy();
        expect(batch[1].snapshotText).toBe('hello world');
      } finally {
        store2.close();
      }
    },
    30000,
  );

  it(
    'second correction builds supersedes lineage and flows to consumer',
    async () => {
      const lease2Res = await runWorker({
        action: 'acquire-lease',
        dbPath,
        sessionId: SESSION_ID,
        sourceId: 'srcA',
        sourceSeq: 1,
        actor: 'dave',
        baseRevision: 2,
        ttlMs: 60000,
      });
      expect(lease2Res.ok).toBe(true);
      const lease2 = lease2Res.lease as { leaseId: string };

      const corr2Res = await runWorker({
        action: 'submit-correction',
        dbPath,
        sessionId: SESSION_ID,
        leaseId: lease2.leaseId,
        correctedContent: 'hello world!',
        reason: 'added punctuation',
      });
      expect(corr2Res.ok).toBe(true);

      const correctionsRes = await runWorker({
        action: 'get-corrections',
        dbPath,
        sessionId: SESSION_ID,
        sourceId: 'srcA',
        sourceSeq: 1,
      });
      const corrections = correctionsRes.corrections as Array<{
        correctionId: string;
        supersedesCorrectionId: string | null;
      }>;
      expect(corrections).toHaveLength(2);
      expect(corrections[1].supersedesCorrectionId).toBe(
        corrections[0].correctionId,
      );

      const consumerRes = await runWorker({
        action: 'consumer-read',
        dbPath,
        sessionId: SESSION_ID,
        consumerId: 'qc-engine',
      });
      expect(consumerRes.ok).toBe(true);
      const revisions = consumerRes.revisions as Array<{
        changeType: string;
        correctionId: string | null;
      }>;
      expect(
        revisions.some((r) => r.changeType === 'correction-applied'),
      ).toBe(true);
    },
    30000,
  );

  it(
    'expired lease conflict is distinguishable after TTL and process restart',
    async () => {
      const ttlDbPath = join(dir, 'ttl.db');
      const ttlSession = 'ttl-session';

      await runWorker({
        action: 'ingest',
        dbPath: ttlDbPath,
        sessionId: ttlSession,
        events: [
          {
            eventId: 'te1',
            sourceId: 'srcA',
            sourceSeq: 1,
            type: 'final',
            content: 'original',
          },
        ],
      });

      const leaseRes = await runWorker({
        action: 'acquire-lease',
        dbPath: ttlDbPath,
        sessionId: ttlSession,
        sourceId: 'srcA',
        sourceSeq: 1,
        actor: 'eve',
        baseRevision: 1,
        ttlMs: 100,
      });
      expect(leaseRes.ok).toBe(true);
      const expiredLease = leaseRes.lease as { leaseId: string };

      await new Promise((r) => setTimeout(r, 200));

      const conflictRes = await runWorker({
        action: 'submit-correction',
        dbPath: ttlDbPath,
        sessionId: ttlSession,
        leaseId: expiredLease.leaseId,
        correctedContent: 'should fail',
        reason: 'late',
      });
      expect(conflictRes.ok).toBe(false);
      expect(conflictRes.code).toBe('LEASE_EXPIRED');

      const store3 = RecognitionStore.open({ dbPath: ttlDbPath });
      try {
        const s3 = store3.session(ttlSession);
        const lease = s3.getLease('srcA', 1);
        expect(lease).not.toBeNull();
        expect(lease!.status).toBe('expired');

        const newLease = s3.acquireLease({
          sourceId: 'srcA',
          sourceSeq: 1,
          actor: 'frank',
          baseRevision: 1,
          ttlMs: 30000,
        });
        expect(newLease.status).toBe('active');
        expect(newLease.actor).toBe('frank');
      } finally {
        store3.close();
      }
    },
    30000,
  );

  it(
    'stale base revision conflict when content changed under lease',
    async () => {
      const staleDbPath = join(dir, 'stale.db');
      const staleSession = 'stale-session';

      await runWorker({
        action: 'ingest',
        dbPath: staleDbPath,
        sessionId: staleSession,
        events: [
          {
            eventId: 'se1',
            sourceId: 'srcA',
            sourceSeq: 1,
            type: 'partial',
            content: 'v1',
          },
        ],
      });

      const store4 = RecognitionStore.open({ dbPath: staleDbPath });
      try {
        const s4 = store4.session(staleSession);
        const lease = s4.acquireLease({
          sourceId: 'srcA',
          sourceSeq: 1,
          actor: 'grace',
          baseRevision: 1,
          ttlMs: 30000,
        });

        s4.ingest({
          eventId: 'se2',
          sourceId: 'srcA',
          sourceSeq: 1,
          type: 'partial',
          content: 'v2-changed',
        });

        let caughtCode: string | undefined;
        try {
          s4.submitCorrection({
            leaseId: lease.leaseId,
            correctedContent: 'corrected',
            reason: 'fix',
          });
        } catch (err) {
          caughtCode = (err as { code?: string }).code;
        }
        expect(caughtCode).toBe('STALE_BASE_REVISION');
      } finally {
        store4.close();
      }
    },
    30000,
  );
});
