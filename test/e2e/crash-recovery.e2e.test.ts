import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { runWorker } from './spawn-helper';
import { forceRemove } from '../cleanup';

const SESSION_ID = 'crash-session';

describe('e2e: crash recovery', () => {
  let dbPath: string;
  let dir: string;

  beforeAll(() => {
    dir = mkdtempSync(join(tmpdir(), 'asr-e2e-crash-'));
    dbPath = join(dir, 'crash.db');
  });

  afterAll(() => {
    forceRemove(dir);
  });

  it(
    'committed transactions survive abrupt process kill',
    async () => {
      const events = [
        {
          eventId: 'e1',
          sourceId: 'srcA',
          sourceSeq: 0,
          type: 'final' as const,
          content: 'hello ',
        },
        {
          eventId: 'e2',
          sourceId: 'srcA',
          sourceSeq: 1,
          type: 'final' as const,
          content: 'world',
        },
      ];

      const result = await runWorker({
        action: 'ingest-then-crash',
        dbPath,
        sessionId: SESSION_ID,
        events,
        crashAfterIndex: 2,
      });
      expect(result.killed).toBe(true);

      const snapRes = await runWorker({
        action: 'snapshot',
        dbPath,
        sessionId: SESSION_ID,
      });
      expect(snapRes.ok).toBe(true);
      const snapshot = snapRes.snapshot as { text: string; revision: number };
      expect(snapshot.text).toBe('hello world');
      expect(snapshot.revision).toBe(2);
    },
    30000,
  );

  it(
    'uncommitted transaction is fully rolled back after kill',
    async () => {
      const crashDbPath = join(dir, 'mid-txn.db');
      const events = [
        {
          eventId: 'mid1',
          sourceId: 'srcA',
          sourceSeq: 0,
          type: 'final' as const,
          content: 'should-rollback',
        },
        {
          eventId: 'mid2',
          sourceId: 'srcA',
          sourceSeq: 1,
          type: 'final' as const,
          content: 'should-also-rollback',
        },
      ];

      const result = await runWorker({
        action: 'crash-mid-txn',
        dbPath: crashDbPath,
        sessionId: 'mid-txn-session',
        events,
      });
      expect(result.killed).toBe(true);

      const countRes = await runWorker({
        action: 'revisions-count',
        dbPath: crashDbPath,
        sessionId: 'mid-txn-session',
      });
      expect(countRes.ok).toBe(true);
      expect(countRes.count).toBe(0);
      expect(countRes.latest).toBe(0);

      const snapRes = await runWorker({
        action: 'snapshot',
        dbPath: crashDbPath,
        sessionId: 'mid-txn-session',
      });
      expect(snapRes.ok).toBe(true);
      const snapshot = snapRes.snapshot as { text: string };
      expect(snapshot.text).toBe('');
    },
    30000,
  );

  it(
    'consumer cursor survives crash and resumes correctly',
    async () => {
      const cursorDbPath = join(dir, 'cursor.db');
      const sessionId = 'cursor-session';
      const consumerId = 'test-consumer';

      for (let i = 0; i < 5; i++) {
        await runWorker({
          action: 'ingest',
          dbPath: cursorDbPath,
          sessionId,
          events: [
            {
              eventId: `ce${i}`,
              sourceId: 'srcA',
              sourceSeq: i,
              type: 'final' as const,
              content: `chunk${i}`,
            },
          ],
        });
      }

      await runWorker({
        action: 'consumer-ack',
        dbPath: cursorDbPath,
        sessionId,
        consumerId,
        revision: 3,
      });

      await runWorker({
        action: 'ingest-then-crash',
        dbPath: cursorDbPath,
        sessionId,
        events: [],
        crashAfterIndex: 0,
      });

      const readRes = await runWorker({
        action: 'consumer-read',
        dbPath: cursorDbPath,
        sessionId,
        consumerId,
      });
      expect(readRes.ok).toBe(true);
      expect(readRes.cursor).toBe(3);
      const revisions = readRes.revisions as Array<{ revision: number }>;
      expect(revisions.map((r) => r.revision)).toEqual([4, 5]);
    },
    30000,
  );

  it(
    'state remains consistent after repeated open/close cycles',
    async () => {
      const cycleDbPath = join(dir, 'cycles.db');
      const sessionId = 'cycle-session';

      for (let cycle = 0; cycle < 5; cycle++) {
        await runWorker({
          action: 'ingest',
          dbPath: cycleDbPath,
          sessionId,
          events: [
            {
              eventId: `cycle-${cycle}`,
              sourceId: 'srcA',
              sourceSeq: cycle,
              type: 'final' as const,
              content: `${cycle},`,
            },
          ],
        });
      }

      const snapRes = await runWorker({
        action: 'snapshot',
        dbPath: cycleDbPath,
        sessionId,
      });
      const snapshot = snapRes.snapshot as { text: string; revision: number };
      expect(snapshot.text).toBe('0,1,2,3,4,');
      expect(snapshot.revision).toBe(5);
    },
    30000,
  );
});
