import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { AsrEvent } from '../src/types';

export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function shuffled<T>(rng: () => number, items: T[]): T[] {
  const out = [...items];
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

export interface Delivery {
  sourceId: string;
  event: AsrEvent;
}

/**
 * Generate a canonical session: `sources` sources, each with `utterances`
 * utterances of 0..2 partials followed by a final. Events are returned
 * per source in ascending sourceSeq order (the canonical order).
 */
export function generateSession(
  rng: () => number,
  sources: number,
  utterances: number,
): Map<string, AsrEvent[]> {
  const words = ['alpha', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf', 'hotel'];
  const word = () => words[Math.floor(rng() * words.length)];
  const bySource = new Map<string, AsrEvent[]>();
  let clock = 0;
  for (let s = 0; s < sources; s++) {
    const sourceId = `mic-${s}`;
    const events: AsrEvent[] = [];
    let seq = 0;
    for (let u = 0; u < utterances; u++) {
      const partialCount = Math.floor(rng() * 3); // 0..2
      const finalText = `${word()} ${word()} ${word()}`;
      for (let p = 0; p < partialCount; p++) {
        events.push({
          eventId: `${sourceId}-e${seq}`,
          sourceSeq: seq,
          kind: 'partial',
          text: finalText.split(' ').slice(0, p + 1).join(' '),
          startMs: clock,
        });
        seq++;
      }
      events.push({
        eventId: `${sourceId}-e${seq}`,
        sourceSeq: seq,
        kind: 'final',
        text: finalText,
        startMs: clock,
      });
      seq++;
      clock += 100 + Math.floor(rng() * 50);
    }
    bySource.set(sourceId, events);
  }
  return bySource;
}

/**
 * Flatten canonical per-source events into one shuffled delivery stream,
 * injecting exact re-deliveries (duplicates) along the way.
 */
export function makeDeliveries(
  rng: () => number,
  bySource: Map<string, AsrEvent[]>,
  duplicateRatio = 0.3,
): Delivery[] {
  const out: Delivery[] = [];
  for (const [sourceId, events] of bySource) {
    for (const event of events) {
      out.push({ sourceId, event });
      if (rng() < duplicateRatio) out.push({ sourceId, event }); // exact re-delivery
    }
  }
  return shuffled(rng, out);
}

export function tmpDbPath(prefix = 'asr-store-'): string {
  return join(mkdtempSync(join(tmpdir(), prefix)), 'test.db');
}
