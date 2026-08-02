export type EventKind = 'partial' | 'final';

/** One emission from an ASR source. eventId is unique per (session, source). */
export interface AsrEvent {
  eventId: string;
  /** Monotonic per (session, source). 0-based or 1-based, caller's choice. */
  sourceSeq: number;
  kind: EventKind;
  text: string;
  /** Optional segment start time in ms; used for deterministic ordering. */
  startMs?: number;
}

export type IngestStatus =
  /** Event changed state and was assigned `revision`. */
  | 'applied'
  /** Exact same eventId + content already stored; no-op. */
  | 'duplicate'
  /** sourceSeq is not newer than what the source already committed; dropped. */
  | 'stale';

export interface IngestResult {
  status: IngestStatus;
  /** Current last revision of the session (new revision when status==='applied'). */
  revision: number;
}

export interface Segment {
  sourceId: string;
  eventId: string;
  sourceSeq: number;
  kind: EventKind;
  text: string;
  startMs: number | null;
}

export interface Summary {
  sources: number;
  finalSegments: number;
  partialSegments: number;
  /** Character count of the joined final text. */
  characters: number;
}

export interface Snapshot {
  sessionId: string;
  /** Last revision of the session at the time of the snapshot. */
  revision: number;
  /** Committed final segments in deterministic order. */
  segments: Segment[];
  /** Current tentative partial per source (at most one per source). */
  partials: Segment[];
  /** Final segment texts joined with a single space. */
  text: string;
  summary: Summary;
}

/** What an applied ingest changed; stored as the payload of a revision. */
export interface ChangeRecord {
  type: EventKind;
  sourceId: string;
  eventId: string;
  sourceSeq: number;
  text: string;
  startMs: number | null;
}

export interface RevisionEntry {
  revision: number;
  change: ChangeRecord;
}

export interface PollResult {
  /** Revisions strictly greater than the consumer's acked cursor. */
  entries: RevisionEntry[];
  /** Consumer's acked cursor at the start of this poll. */
  ackedRevision: number;
  /** Session's last revision at the start of this poll. */
  lastRevision: number;
}
