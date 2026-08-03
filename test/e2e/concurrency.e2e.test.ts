import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { runWorker } from './spawn-helper';
import { forceRemove } from '../cleanup';
import type { IngestEvent } from '../../src';

const SESSION_ID = 'concurrency-session';

function makeEvents(
  sourceId: string,
  count: number,
  startSeq: number,
): IngestEvent[] {
  const events: IngestEvent[] = [];
  for (let i = 0; i < count; i++) {
    const seq = startSeq + i;
    events.push({
      eventId: `${sourceId}-${seq}-final`,
      sourceId,
      sourceSeq: seq,
      type: 'final',
      content: `[${sourceId}#${seq}]`,
    });
  }
  return events;
}

describe('e2e: multi-instance concurrency', () => {
  let dbPath: string;
  let dir: string;

  beforeAll(() => {
    dir = mkdtempSync(join(tmpdir(), 'asr-e2e-conc-'));
    dbPath = join(dir, 'concurrency.db');
  });

  afterAll(() => {
    forceRemove(dir);
  });

  it(
    'multiple processes writing to the same DB produce consistent final state',
    async () => {
      const sources = ['alpha', 'bravo', 'charlie', 'delta'];
      const perSource = 25;
      const expectedText = sources
        .sort()
        .map((s) =>
          Array.from({ length: perSource }, (_, i) => `[${s}#${i}]`).join(''),
        )
        .join('');
      const expectedRevisionCount = sources.length * perSource;

      const workerPromises = sources.map((sourceId) =>
        runWorker({
          action: 'ingest',
          dbPath,
          sessionId: SESSION_ID,
          events: makeEvents(sourceId, perSource, 0),
        }),
      );

      const results = await Promise.all(workerPromises);
      for (const r of results) {
        expect(r.ok).toBe(true);
      }

      const snapRes = await runWorker({
        action: 'snapshot',
        dbPath,
        sessionId: SESSION_ID,
      });
      expect(snapRes.ok).toBe(true);
      const snapshot = snapRes.snapshot as { text: string; revision: number };
      expect(snapshot.text).toBe(expectedText);
      expect(snapshot.revision).toBe(expectedRevisionCount);

      const countRes = await runWorker({
        action: 'revisions-count',
        dbPath,
        sessionId: SESSION_ID,
      });
      expect(countRes.ok).toBe(true);
      expect(countRes.count).toBe(expectedRevisionCount);
      expect(countRes.latest).toBe(expectedRevisionCount);
    },
    60000,
  );

  it(
    'deterministic result regardless of process interleaving (multiple runs)',
    async () => {
      const runOnce = async (runId: number) => {
        const runDir = mkdtempSync(join(tmpdir(), `asr-e2e-det-${runId}-`));
        const runDb = join(runDir, 'det.db');
        try {
          const sources = ['srcA', 'srcB', 'srcC'];
          const allEvents = sources.flatMap((s) =>
            makeEvents(s, 10, 0),
          );
          for (let i = allEvents.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [allEvents[i], allEvents[j]] = [allEvents[j], allEvents[i]];
          }
          const chunkSize = Math.ceil(allEvents.length / 3);
          const chunks = [
            allEvents.slice(0, chunkSize),
            allEvents.slice(chunkSize, chunkSize * 2),
            allEvents.slice(chunkSize * 2),
          ];
          await Promise.all(
            chunks.map((events) =>
              runWorker({
                action: 'ingest',
                dbPath: runDb,
                sessionId: 'det',
                events,
              }),
            ),
          );
          const res = await runWorker({
            action: 'snapshot',
            dbPath: runDb,
            sessionId: 'det',
          });
          return (res.snapshot as { text: string }).text;
        } finally {
          forceRemove(runDir);
        }
      };

      const baseline = await runOnce(0);
      for (let i = 1; i < 4; i++) {
        const result = await runOnce(i);
        expect(result).toBe(baseline);
      }
    },
    120000,
  );
});
