export const ARCHIVE_FORMAT_VERSION = 1;
export const ARCHIVE_MAGIC = "ASR-SESSION-ARCHIVE";

export type ArchiveRecordType =
  | "event"
  | "revision"
  | "correction"
  | "lease"
  | "cursor"
  | string;

export interface ArchiveHeader {
  magic: typeof ARCHIVE_MAGIC;
  archiveVersion: typeof ARCHIVE_FORMAT_VERSION;
  libraryVersion: number;
  sessionId: string;
  createdAt: number;
  recordCount: number;
  dataHashAlgorithm: "sha256";
  dataHash: string;
  prevArchiveHash?: string;
  baseCheckpointId?: string;
  checkpointId?: string;
}

export interface ArchiveEventRecord {
  eventId: string;
  sourceId: string;
  sourceSeq: number;
  kind: "final" | "partial";
  text: string;
  createdAt: number;
}

export interface ArchiveRevisionRecord {
  revision: number;
  changeType: "event" | "correction";
  eventId?: string;
  sourceId?: string;
  sourceSeq?: number;
  kind?: "final" | "partial";
  correctionId?: string;
  snapshot: unknown;
  createdAt: number;
}

export interface ArchiveCorrectionRecord {
  correctionId: string;
  sourceId: string;
  sourceSeq: number;
  actor: string;
  reason: string;
  originalText: string;
  correctedText: string;
  baseRevision: number;
  supersedes?: string;
  leaseId: string;
  createdAt: number;
}

export interface ArchiveLeaseRecord {
  leaseId: string;
  sourceId: string;
  sourceSeq: number;
  actor: string;
  baseRevision: number;
  ttlMs: number;
  claimedAt: number;
  expiresAt: number;
  releasedAt?: number;
}

export interface ArchiveCursorRecord {
  consumerId: string;
  cursor: number;
  updatedAt: number;
  leaseTtlMs?: number;
  lastSeenAt?: number;
  resetToCheckpoint?: string;
}

export interface ArchiveCheckpointRecord {
  checkpointId: string;
  revision: number;
  archiveHash: string;
  prevArchiveHash?: string;
  recordCount: number;
  createdAt: number;
}

export interface ArchiveCompactionStateRecord {
  compactedThroughRevision: number;
  baselineRevision: number;
  lastCheckpointId?: string;
  lastCompactedAt?: number;
  totalRevisionsRemoved: number;
  totalEventsRemoved: number;
  totalCorrectionsRemoved: number;
  totalLeasesRemoved: number;
}

export type ArchiveKnownData =
  | { type: "event"; data: ArchiveEventRecord }
  | { type: "revision"; data: ArchiveRevisionRecord }
  | { type: "correction"; data: ArchiveCorrectionRecord }
  | { type: "lease"; data: ArchiveLeaseRecord }
  | { type: "cursor"; data: ArchiveCursorRecord }
  | { type: "checkpoint"; data: ArchiveCheckpointRecord }
  | { type: "compaction"; data: ArchiveCompactionStateRecord };

export interface ArchiveUnknownData {
  type: string;
  data: Record<string, unknown>;
}

export type ArchiveDataRecord = ArchiveKnownData | ArchiveUnknownData;

export interface ArchiveFooter {
  end: true;
  recordCount: number;
  dataHash: string;
}

export interface ArchiveImportResult {
  sessionId: string;
  imported: {
    events: number;
    revisions: number;
    corrections: number;
    leases: number;
    cursors: number;
    checkpoints: number;
    unknown: number;
  };
  duplicates: {
    events: number;
    revisions: number;
    corrections: number;
    leases: number;
    cursors: number;
    checkpoints: number;
  };
  conflicting: number;
}

export interface SessionArchiveStream {
  [Symbol.asyncIterator](): AsyncIterableIterator<string>;
}
