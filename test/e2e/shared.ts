import type { SourceEvent } from "../../src/index";

/**
 * Deterministic scenario used by both the e2e test drivers and the worker
 * child processes, so a baseline computed in-process can be compared against
 * the result of many concurrent OS processes writing the same logical stream.
 */
export function makeEvents(sessionId: string, sources: number, segs: number, partials: number): SourceEvent[] {
  const events: SourceEvent[] = [];
  for (let s = 0; s < sources; s++) {
    const sourceId = `src-${s}`;
    let seq = 0;
    for (let g = 0; g < segs; g++) {
      const segmentId = `seg-${g}`;
      const startMs = g * 1000 + s;
      for (let p = 1; p <= partials; p++) {
        seq += 1;
        events.push({
          sessionId,
          sourceId,
          segmentId,
          eventId: `${sourceId}:${segmentId}:p${p}`,
          sourceSeq: seq,
          kind: "partial",
          text: `${sourceId}/${segmentId} partial#${p}`,
          startMs,
          endMs: startMs + p * 50,
        });
      }
      seq += 1;
      events.push({
        sessionId,
        sourceId,
        segmentId,
        eventId: `${sourceId}:${segmentId}:final`,
        sourceSeq: seq,
        kind: "final",
        text: `${sourceId}/${segmentId} FINAL`,
        startMs,
        endMs: startMs + 1000,
      });
    }
  }
  return events;
}

/** mulberry32 deterministic RNG. */
export function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function shuffle<T>(arr: T[], rand: () => number): T[] {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [a[i], a[j]] = [a[j]!, a[i]!];
  }
  return a;
}

/** Order-independent fingerprint of resolved segment state + summary. */
export function fingerprint(segments: Array<Record<string, unknown>>, summary: unknown): string {
  const norm = segments
    .map((s) => ({
      sourceId: s.sourceId,
      segmentId: s.segmentId,
      kind: s.kind,
      text: s.text,
      sourceSeq: s.sourceSeq,
      eventId: s.eventId,
    }))
    .sort((a, b) => {
      const k1 = `${a.sourceId}|${a.segmentId}`;
      const k2 = `${b.sourceId}|${b.segmentId}`;
      return k1 < k2 ? -1 : k1 > k2 ? 1 : 0;
    });
  return JSON.stringify({ norm, summary });
}
