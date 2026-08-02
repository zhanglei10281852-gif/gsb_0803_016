/**
 * Public types for the ASR revision hub.
 *
 * A "session" is one logical conversation. Within a session, several
 * independent "sources" (recognizers) each emit a stream of events. Every
 * event targets a "segment" (an utterance) identified by `segmentId` inside
 * that source. A source refines a segment with `partial` events and closes it
 * with a `final` event.
 */

/** Kind of transcript event a source can emit for a segment. */
export type EventKind = "partial" | "final";

/**
 * Provenance of a revision in the durable stream:
 *  - "recognition": produced by applying a raw source event.
 *  - "correction":  produced by a human reviewer submitting a correction.
 */
export type RevisionOrigin = "recognition" | "correction";

/**
 * A single recognizer output for one segment.
 *
 * Identity rules:
 *  - `eventId` is stable and unique within `(sessionId, sourceId)`. The same
 *    `eventId` re-delivered with identical content is idempotent; re-delivered
 *    with different content is a hard conflict.
 *  - `sourceSeq` is monotonically increasing within a source and is used to
 *    decide which event is newer when resolving a segment.
 */
export interface SourceEvent {
  sessionId: string;
  sourceId: string;
  segmentId: string;
  eventId: string;
  sourceSeq: number;
  kind: EventKind;
  text: string;
  /** Optional segment start time in ms; used for deterministic ordering. */
  startMs?: number;
  /** Optional segment end time in ms. */
  endMs?: number;
}

/** Outcome of applying one event. */
export type ApplyOutcome =
  /** First event ever seen for this segment; snapshot gained a segment. */
  | "created"
  /** Event became the new winner for an existing segment; snapshot changed. */
  | "updated"
  /** Exact re-delivery of a known event; nothing changed. */
  | "duplicate"
  /** Event was recorded but did not win (stale partial, or late partial after
   *  a final, or lower precedence); snapshot unchanged. */
  | "superseded";

/** Result of {@link RevisionHub.apply}. */
export interface ApplyResult {
  sessionId: string;
  sourceId: string;
  segmentId: string;
  eventId: string;
  outcome: ApplyOutcome;
  /** True when the snapshot changed and a new revision was assigned. */
  effective: boolean;
  /** Revision number assigned when `effective`, otherwise null. */
  revision: number | null;
}

/** Resolved state of a single segment in a snapshot. */
export interface SegmentState {
  sourceId: string;
  segmentId: string;
  kind: EventKind;
  text: string;
  startMs: number | null;
  endMs: number | null;
  /** `sourceSeq` of the event currently representing this segment. */
  sourceSeq: number;
  /** `eventId` of the event currently representing this segment. */
  eventId: string;
  /** Revision at which this segment reached its current state. */
  revision: number;
  /**
   * Where the current text came from: raw recognition ("recognition") or a
   * human correction ("correction"). Corrections are authoritative and are
   * never rolled back by later recognition events.
   */
  origin: RevisionOrigin;
  /** Actor who produced the current state, when it came from a correction. */
  actor: string | null;
}

/** Aggregated, deterministic summary of a session snapshot. */
export interface SessionSummary {
  segmentCount: number;
  finalCount: number;
  partialCount: number;
  /** Full transcript: segment texts joined in snapshot order. */
  fullText: string;
  /** Per-source counts, keyed by sourceId (sorted keys). */
  perSource: Record<string, { segments: number; finals: number; partials: number }>;
}

/** A queryable, point-in-time view of a session. */
export interface Snapshot {
  sessionId: string;
  /** Highest revision assigned in this session (0 if none). */
  headRevision: number;
  /** Segments in deterministic order (startMs, sourceId, segmentId). */
  segments: SegmentState[];
  summary: SessionSummary;
}

/** One entry in the durable revision stream. */
export interface RevisionRecord {
  sessionId: string;
  revision: number;
  sourceId: string;
  segmentId: string;
  kind: EventKind;
  text: string;
  startMs: number | null;
  endMs: number | null;
  /** The event that caused this revision. */
  eventId: string;
  sourceSeq: number;
  /** Wall-clock ms when the revision was recorded. */
  createdAt: number;
  /** Provenance: "recognition" for source events, "correction" for reviews. */
  origin: RevisionOrigin;
  /** Reviewer identity for correction revisions; null for recognition. */
  actor: string | null;
  /** Reason supplied by the reviewer; null for recognition. */
  reason: string | null;
  /** Stable correction id; null for recognition. */
  correctionId: string | null;
  /** The revision this correction superseded (its baseRevision); null otherwise. */
  supersedesRevision: number | null;
}

/** Options for {@link RevisionHub.pull}. */
export interface PullOptions {
  /** Max records to return. Default 100. */
  limit?: number;
  /**
   * Read strictly after this revision instead of the consumer's stored cursor.
   * The stored cursor is not modified by pull; use {@link RevisionHub.ack}.
   */
  afterRevision?: number;
}

/** Options for constructing a {@link RevisionHub}. */
export interface RevisionHubOptions {
  /** SQLite file path, or ":memory:" for an ephemeral store. */
  path: string;
  /**
   * SQLite synchronous mode. "FULL" (default) survives OS/power crashes;
   * "NORMAL" is faster and still survives process crashes under WAL.
   */
  synchronous?: "FULL" | "NORMAL";
  /** busy_timeout in ms for cross-process write contention. Default 5000. */
  busyTimeoutMs?: number;
  /**
   * Time source in epoch ms. Defaults to `Date.now`. Injectable so lease TTL
   * expiry is testable deterministically without real sleeps.
   */
  clock?: () => number;
}

/** A segment address: which recognizer segment a review targets. */
export interface SegmentRef {
  sessionId: string;
  sourceId: string;
  segmentId: string;
}

/** Request to acquire a time-boxed correction lease on a segment. */
export interface AcquireLeaseRequest extends SegmentRef {
  /** Reviewer identity taking the lease. */
  actor: string;
  /**
   * The revision the reviewer based their view on. The correction will only be
   * accepted if the segment is still at this revision at submit time.
   */
  baseRevision: number;
  /** Lease lifetime in ms from acquisition. Must be a positive integer. */
  ttlMs: number;
}

/** A granted correction lease. */
export interface Lease extends SegmentRef {
  leaseId: string;
  actor: string;
  baseRevision: number;
  /** Epoch ms when acquired. */
  acquiredAt: number;
  /** Epoch ms when the lease expires (acquiredAt + ttlMs). */
  expiresAt: number;
}

/** Request to submit a correction under a previously granted lease. */
export interface SubmitCorrectionRequest {
  leaseId: string;
  /** Must match the lease holder. */
  actor: string;
  /** The corrected text for the segment. */
  text: string;
  /** Human-readable reason / justification for the correction. */
  reason: string;
  /** Optional idempotency key; a re-submit with the same id is idempotent. */
  correctionId?: string;
}

/** Result of a successful {@link RevisionHub.submitCorrection}. */
export interface CorrectionResult extends SegmentRef {
  correctionId: string;
  actor: string;
  /** The new revision assigned to this correction. */
  revision: number;
  /** The revision it superseded (the lease's baseRevision). */
  supersedesRevision: number;
  /** True when this was a fresh apply; false when an idempotent replay. */
  applied: boolean;
}
