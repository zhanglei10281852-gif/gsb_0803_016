import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SourceEvent } from "../src/index";

/** Create a throwaway directory for a SQLite file; returns path + cleanup. */
export function tempDbPath(prefix = "asrhub-"): { path: string; cleanup: () => void } {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  return {
    path: join(dir, "hub.sqlite"),
    cleanup: () => rmSync(dir, { recursive: true, force: true }),
  };
}

let seedCounter = 1;

/** Deterministic pseudo-random generator (mulberry32) for repeatable shuffles. */
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

/** In-place Fisher-Yates shuffle driven by a seeded rng. */
export function shuffle<T>(arr: T[], rand: () => number): T[] {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [a[i], a[j]] = [a[j]!, a[i]!];
  }
  return a;
}

export interface ScenarioOptions {
  sessionId?: string;
  sources?: number;
  segmentsPerSource?: number;
  partialsPerSegment?: number;
  finalize?: boolean;
  /** Include duplicate re-deliveries of some events. */
  duplicates?: boolean;
  seed?: number;
}

/**
 * Build a realistic multi-source event set where each segment goes through a
 * sequence of partials and (optionally) a final. eventIds and sourceSeqs are
 * stable so the set can be shuffled/duplicated and still resolve identically.
 */
export function buildScenario(opts: ScenarioOptions = {}): {
  sessionId: string;
  events: SourceEvent[];
} {
  const sessionId = opts.sessionId ?? `sess-${seedCounter++}`;
  const sources = opts.sources ?? 3;
  const segs = opts.segmentsPerSource ?? 4;
  const partials = opts.partialsPerSegment ?? 3;
  const finalize = opts.finalize ?? true;
  const withDup = opts.duplicates ?? false;

  const events: SourceEvent[] = [];
  for (let s = 0; s < sources; s++) {
    const sourceId = `src-${s}`;
    let seq = 0;
    for (let g = 0; g < segs; g++) {
      const segmentId = `seg-${g}`;
      const startMs = g * 1000 + s * 10;
      for (let p = 1; p <= partials; p++) {
        seq += 1;
        events.push({
          sessionId,
          sourceId,
          segmentId,
          eventId: `${sourceId}:${segmentId}:p${p}`,
          sourceSeq: seq,
          kind: "partial",
          text: `s${s}g${g} partial ${p}`,
          startMs,
          endMs: startMs + p * 100,
        });
      }
      if (finalize) {
        seq += 1;
        events.push({
          sessionId,
          sourceId,
          segmentId,
          eventId: `${sourceId}:${segmentId}:final`,
          sourceSeq: seq,
          kind: "final",
          text: `s${s}g${g} FINAL`,
          startMs,
          endMs: startMs + 1000,
        });
      }
    }
  }

  if (withDup) {
    const rand = rng(opts.seed ?? 12345);
    const dups = events.filter(() => rand() < 0.3).map((e) => ({ ...e }));
    events.push(...dups);
  }

  return { sessionId, events };
}
