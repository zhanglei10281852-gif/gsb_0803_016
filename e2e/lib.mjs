import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

export function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function shuffled(rng, items) {
  const out = [...items];
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

export function generateSession(rng, sources, utterances) {
  const words = ['alpha', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot'];
  const word = () => words[Math.floor(rng() * words.length)];
  const bySource = new Map();
  let clock = 0;
  for (let s = 0; s < sources; s++) {
    const sourceId = `mic-${s}`;
    const events = [];
    let seq = 0;
    for (let u = 0; u < utterances; u++) {
      const finalText = `${word()} ${word()} ${word()}`;
      const partialCount = Math.floor(rng() * 3);
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

export function makeDeliveries(rng, bySource, duplicateRatio = 0.3) {
  const out = [];
  for (const [sourceId, events] of bySource) {
    for (const event of events) {
      out.push({ sourceId, event });
      if (rng() < duplicateRatio) out.push({ sourceId, event });
    }
  }
  return shuffled(rng, out);
}

export function tmpDbPath(prefix) {
  return join(mkdtempSync(join(tmpdir(), prefix)), 'e2e.db');
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Snapshot without the sessionId field, for deep-equal comparison. */
export function comparable({ sessionId, ...rest }) {
  return rest;
}
