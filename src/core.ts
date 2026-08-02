import { createHash } from 'node:crypto';
import type { AsrEvent, Segment, Snapshot } from './types';

/** Content fingerprint used to distinguish duplicates from conflicts. */
export function eventHash(e: Pick<AsrEvent, 'kind' | 'text' | 'startMs'>): string {
  return createHash('sha256')
    .update(JSON.stringify([e.kind, e.text, e.startMs ?? null]))
    .digest('hex');
}

/** Row shape shared by the SQLite store and the in-memory reference model. */
export interface EventRow {
  source_id: string;
  event_id: string;
  source_seq: number;
  kind: 'partial' | 'final';
  text: string;
  start_ms: number | null;
}

const NO_START = Number.MAX_SAFE_INTEGER;

/**
 * Deterministic, arrival-order-independent ordering for final segments:
 * (startMs, sourceId, sourceSeq, eventId). Segments without startMs sort last.
 */
export function compareFinals(a: EventRow, b: EventRow): number {
  const sa = a.start_ms ?? NO_START;
  const sb = b.start_ms ?? NO_START;
  if (sa !== sb) return sa - sb;
  if (a.source_id !== b.source_id) return a.source_id < b.source_id ? -1 : 1;
  if (a.source_seq !== b.source_seq) return a.source_seq - b.source_seq;
  if (a.event_id === b.event_id) return 0;
  return a.event_id < b.event_id ? -1 : 1;
}

/** Partials are tentative; order them by source then seq for a stable view. */
export function comparePartials(a: EventRow, b: EventRow): number {
  if (a.source_id !== b.source_id) return a.source_id < b.source_id ? -1 : 1;
  if (a.source_seq !== b.source_seq) return a.source_seq - b.source_seq;
  if (a.event_id === b.event_id) return 0;
  return a.event_id < b.event_id ? -1 : 1;
}

function toSegment(r: EventRow): Segment {
  return {
    sourceId: r.source_id,
    eventId: r.event_id,
    sourceSeq: r.source_seq,
    kind: r.kind,
    text: r.text,
    startMs: r.start_ms,
  };
}

export function buildSnapshot(sessionId: string, revision: number, rows: EventRow[]): Snapshot {
  const finals = rows.filter((r) => r.kind === 'final').sort(compareFinals);
  const partials = rows.filter((r) => r.kind === 'partial').sort(comparePartials);
  const text = finals.map((f) => f.text).join(' ');
  return {
    sessionId,
    revision,
    segments: finals.map(toSegment),
    partials: partials.map(toSegment),
    text,
    summary: {
      sources: new Set(rows.map((r) => r.source_id)).size,
      finalSegments: finals.length,
      partialSegments: partials.length,
      characters: text.length,
    },
  };
}
