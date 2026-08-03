export type EventType = 'partial' | 'final';

export type ChangeType =
  | 'partial-updated'
  | 'final-committed'
  | 'correction-applied';

export type IngestStatus = 'accepted' | 'duplicate' | 'ignored';

export type LeaseStatus = 'active' | 'consumed' | 'expired' | 'released';

export interface IngestEvent {
  eventId: string;
  sourceId: string;
  sourceSeq: number;
  type: EventType;
  content: string;
}

export interface IngestResult {
  status: IngestStatus;
  revision: number | null;
  reason: string | null;
}

export interface FragmentData {
  sourceId: string;
  sourceSeq: number;
  type: EventType;
  content: string;
  corrected: boolean;
}

export interface SourceSnapshot {
  sourceId: string;
  fragments: FragmentData[];
}

export interface SessionSummary {
  sourceCount: number;
  fragmentCount: number;
  finalCount: number;
  partialCount: number;
  correctedCount: number;
  textLength: number;
}

export interface Snapshot {
  sessionId: string;
  revision: number;
  sources: SourceSnapshot[];
  text: string;
  summary: SessionSummary;
}

export interface CorrectionMetadata {
  actor: string;
  reason: string;
  originalContent: string;
  supersedesCorrectionId: string | null;
}

export interface RevisionData {
  sessionId: string;
  revision: number;
  eventId: string | null;
  sourceId: string;
  sourceSeq: number;
  changeType: ChangeType;
  content: string;
  snapshotText: string;
  summary: SessionSummary;
  createdAt: number;
  correctionId: string | null;
  correction: CorrectionMetadata | null;
}

export interface StoreOptions {
  dbPath: string;
  busyTimeoutMs?: number;
}

export interface IncomingEventRow {
  session_id: string;
  event_id: string;
  source_id: string;
  source_seq: number;
  event_type: EventType;
  content: string;
  content_hash: string;
  resulted_revision: number | null;
  received_at: number;
}

export interface FragmentRow {
  session_id: string;
  source_id: string;
  source_seq: number;
  event_type: EventType;
  content: string;
  event_id: string;
  is_corrected: number;
  updated_at: number;
}

export interface RevisionRow {
  session_id: string;
  revision: number;
  event_id: string | null;
  source_id: string;
  source_seq: number;
  change_type: ChangeType;
  content: string;
  snapshot_text: string;
  summary: string;
  created_at: number;
  correction_id: string | null;
  metadata: string | null;
}

export interface CursorRow {
  session_id: string;
  consumer_id: string;
  cursor_revision: number;
  updated_at: number;
  lease_expires_at: number | null;
}

export interface LeaseRow {
  session_id: string;
  source_id: string;
  source_seq: number;
  lease_id: string;
  actor: string;
  base_revision: number;
  base_content: string;
  status: LeaseStatus;
  acquired_at: number;
  expires_at: number;
  consumed_at: number | null;
}

export interface CorrectionRow {
  session_id: string;
  correction_id: string;
  lease_id: string;
  source_id: string;
  source_seq: number;
  actor: string;
  reason: string;
  original_content: string;
  corrected_content: string;
  supersedes_correction_id: string | null;
  revision: number;
  created_at: number;
}

export interface Lease {
  leaseId: string;
  sessionId: string;
  sourceId: string;
  sourceSeq: number;
  actor: string;
  baseRevision: number;
  baseContent: string;
  status: LeaseStatus;
  acquiredAt: number;
  expiresAt: number;
  consumedAt: number | null;
}

export interface Correction {
  correctionId: string;
  leaseId: string;
  sessionId: string;
  sourceId: string;
  sourceSeq: number;
  actor: string;
  reason: string;
  originalContent: string;
  correctedContent: string;
  supersedesCorrectionId: string | null;
  revision: number;
  createdAt: number;
}

export interface AcquireLeaseOptions {
  sourceId: string;
  sourceSeq: number;
  actor: string;
  baseRevision: number;
  ttlMs?: number;
}

export interface SubmitCorrectionOptions {
  leaseId: string;
  correctedContent: string;
  reason: string;
}

export interface CheckpointRow {
  session_id: string;
  checkpoint_id: string;
  archive_path: string;
  archive_sha256: string;
  min_revision: number;
  max_revision: number;
  archived_at: number;
  revisions_count: number;
  events_count: number;
}

export interface Checkpoint {
  checkpointId: string;
  sessionId: string;
  archivePath: string;
  archiveSha256: string;
  minRevision: number;
  maxRevision: number;
  archivedAt: number;
  revisionsCount: number;
  eventsCount: number;
}

export interface CompactionStats {
  sessionId: string;
  compacted: boolean;
  safeRevision: number;
  archivedRevision: number;
  slowestActiveCursor: number | null;
  revisionsRemoved: number;
  eventsRemoved: number;
  revisionsRemaining: number;
  eventsRemaining: number;
  checkpointId: string | null;
  skippedReason: string | null;
}

export interface ConsumerLeaseInfo {
  consumerId: string;
  cursorRevision: number;
  leaseExpiresAt: number | null;
  active: boolean;
}

export interface ConsumerOptions {
  leaseTtlMs?: number;
}
