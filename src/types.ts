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
  /** Effective text: the latest correction when one exists, else the original. */
  text: string;
  startMs: number | null;
  /** Original ASR text; only present when the segment was corrected. */
  originalText?: string;
  /** Latest human correction applied to this segment, if any. */
  correction?: CorrectionInfo;
}

/** A human correction. Original events are never rewritten; corrections form a supersedes chain. */
export interface CorrectionInfo {
  correctionId: string;
  text: string;
  actor: string;
  reason: string;
  /** Stream revision at which this correction was committed. */
  revision: number;
  /** correctionId of the previous correction for the same segment, or null. */
  supersedes: string | null;
}

export interface Summary {
  sources: number;
  finalSegments: number;
  partialSegments: number;
  /** Character count of the joined (effective) final text. */
  characters: number;
  /** Segments carrying a human correction. */
  corrections: number;
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
  /** 'partial' | 'final' come from ASR ingest; 'correction' from human review. */
  type: EventKind | 'correction';
  sourceId: string;
  eventId: string;
  sourceSeq: number;
  /** Event text, or the corrected text for corrections. */
  text: string;
  startMs: number | null;
  /** Correction-only fields (absent for ASR events). */
  correctionId?: string;
  actor?: string;
  reason?: string;
  supersedes?: string | null;
  /** Base revision the correction was submitted against (null for pre-archive rows). */
  baseRevision?: number | null;
}

/** Reference to a stored segment, used as the target of leases/corrections. */
export interface SegmentRef {
  sourceId: string;
  eventId: string;
}

export interface AcquireLeaseOptions {
  /** Reviewer identity; recorded on the lease and the correction. */
  actor: string;
  /** Session revision the reviewer's view is based on. */
  baseRevision: number;
  /** Lease time-to-live in milliseconds. */
  ttlMs: number;
}

export interface LeaseInfo {
  leaseId: string;
  expiresAt: number;
}

export interface SubmitCorrectionOptions {
  leaseId: string;
  /** Must match the lease's baseRevision and cover the target's current version. */
  baseRevision: number;
  text: string;
  actor: string;
  reason: string;
}

export interface SubmitCorrectionResult {
  status: 'applied';
  revision: number;
  correctionId: string;
}

export interface ImportResult {
  /** 'imported' on first import; 'duplicate' when this archive was already imported. */
  status: 'imported' | 'duplicate';
  sessionId: string;
  archiveId: string;
  lastRevision: number;
}

export interface ConsumerLeaseOptions {
  /** Lease time-to-live in milliseconds; renew by registering again. */
  ttlMs: number;
}

export interface ConsumerLeaseInfo {
  expiresAt: number;
}

export interface CompactionStats {
  sessionId: string;
  /** Revision-stream entries deleted by this compaction. */
  reclaimedRevisions: number;
  /** Approximate payload bytes of the reclaimed entries. */
  bytesReclaimed: number;
  /** Smallest revision still in the stream after compaction. */
  firstRevision: number;
  lastRevision: number;
  /** Revision covered by the checkpoint created by this compaction. */
  checkpointRevision: number | null;
  checkpointArchiveId: string | null;
  checkpointSha256: string | null;
  /** Consumers with a live lease (they bound the reclaim floor). */
  liveConsumers: number;
  /** Registered consumers in total (live + expired). */
  totalConsumers: number;
}

export interface CheckpointInfo {
  revision: number;
  archiveId: string;
  sha256: string;
  createdAt: number;
}

export interface ConsumerStatus {
  consumerId: string;
  ackedRevision: number;
  leaseExpiresAt: number | null;
  leaseLive: boolean;
}

export interface StorageStats {
  sessionId: string;
  firstRevision: number;
  lastRevision: number;
  /** Revision entries currently stored (lastRevision - firstRevision + 1). */
  storedRevisions: number;
  checkpoints: CheckpointInfo[];
  consumers: ConsumerStatus[];
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
