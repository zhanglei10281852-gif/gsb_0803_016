import type { Readable } from "node:stream";
import type {
  ArchiveImportResult,
  SessionArchiveStream,
} from "./archive-types";
import type { ExportArchiveOptions } from "./archive";

export interface RecognitionEventInput {
  eventId: string;
  sourceId: string;
  sourceSeq: number;
  isFinal: boolean;
  text: string;
}

export type SegmentKind = "final" | "partial";

export interface NormalizedRecognitionEvent {
  eventId: string;
  sourceId: string;
  sourceSeq: number;
  kind: SegmentKind;
  text: string;
}

export interface CorrectionInfo {
  correctionId: string;
  actor: string;
  reason: string;
  originalText: string;
  correctedText: string;
  baseRevision: number;
  supersedes?: string;
  leaseId: string;
  createdAt: number;
}

export interface Segment {
  sourceId: string;
  sourceSeq: number;
  kind: SegmentKind;
  text: string;
  originalText: string;
  correction?: CorrectionInfo;
}

export interface SnapshotSummary {
  revision: number;
  eventCount: number;
  sourceCount: number;
  finalSegmentCount: number;
  activePartialCount: number;
  correctedSegmentCount: number;
  textLength: number;
  finalTextLength: number;
}

export interface Snapshot {
  sessionId: string;
  revision: number;
  segments: Segment[];
  text: string;
  finalText: string;
  summary: SnapshotSummary;
}

export type IngestStatus = "inserted" | "duplicate" | "stale";

export interface IngestOutcome {
  status: IngestStatus;
  eventId: string;
  revision?: number;
}

export type ChangeType = "event" | "correction";

export interface RevisionRecord {
  sessionId: string;
  revision: number;
  changeType: ChangeType;
  eventId?: string;
  sourceId?: string;
  sourceSeq?: number;
  kind?: SegmentKind;
  correctionId?: string;
  snapshot: Snapshot;
  createdAt: number;
}

export interface ReviewLease {
  sessionId: string;
  leaseId: string;
  sourceId: string;
  sourceSeq: number;
  actor: string;
  baseRevision: number;
  ttlMs: number;
  claimedAt: number;
  expiresAt: number;
  releasedAt?: number;
  active: boolean;
}

export interface ClaimLeaseInput {
  sourceId: string;
  sourceSeq: number;
  actor: string;
  baseRevision: number;
  ttlMs: number;
}

export interface ClaimLeaseOutcome {
  lease: ReviewLease;
  acquired: boolean;
}

export interface SubmitCorrectionInput {
  leaseId: string;
  actor: string;
  reason: string;
  correctedText: string;
  baseRevision: number;
}

export interface SubmitCorrectionOutcome {
  correction: CorrectionRecord;
  revision: number;
}

export interface CorrectionRecord {
  sessionId: string;
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

export interface RecognitionStore {
  ingest(
    sessionId: string,
    event: RecognitionEventInput | readonly RecognitionEventInput[],
  ): IngestOutcome | IngestOutcome[];
  getSnapshot(sessionId: string): Snapshot;
  getSnapshotAt(sessionId: string, revision: number): Snapshot;
  getRevision(sessionId: string, revision: number): RevisionRecord;
  fetchChanges(
    sessionId: string,
    consumerId: string,
    limit?: number,
  ): RevisionRecord[];
  acknowledge(sessionId: string, consumerId: string, revision: number): number;
  getCursor(sessionId: string, consumerId: string): number;
  createConsumer(sessionId: string, consumerId: string): RevisionConsumer;

  claimLease(sessionId: string, input: ClaimLeaseInput): ClaimLeaseOutcome;
  releaseLease(sessionId: string, leaseId: string, actor: string): void;
  getLease(sessionId: string, leaseId: string): ReviewLease | undefined;
  getActiveLease(
    sessionId: string,
    sourceId: string,
    sourceSeq: number,
  ): ReviewLease | undefined;
  submitCorrection(
    sessionId: string,
    input: SubmitCorrectionInput,
  ): SubmitCorrectionOutcome;
  getCorrection(
    sessionId: string,
    correctionId: string,
  ): CorrectionRecord | undefined;
  getCorrectionLineage(
    sessionId: string,
    sourceId: string,
    sourceSeq: number,
  ): CorrectionRecord[];

  exportArchive(
    sessionId: string,
    options?: ExportArchiveOptions,
  ): SessionArchiveStream;
  writeArchive(
    sessionId: string,
    outputPath: string,
    options?: ExportArchiveOptions,
  ): Promise<void>;
  importArchive(
    sessionId: string,
    stream: Readable,
    options?: { injectFailureAfterRecords?: number },
  ): Promise<ArchiveImportResult>;
  importArchiveFile(
    sessionId: string,
    inputPath: string,
    options?: { injectFailureAfterRecords?: number },
  ): Promise<ArchiveImportResult>;

  registerCheckpoint(
    sessionId: string,
    input: RegisterCheckpointInput,
  ): ArchiveCheckpoint;
  getCheckpoints(sessionId: string): ArchiveCheckpoint[];
  getCheckpoint(
    sessionId: string,
    checkpointId: string,
  ): ArchiveCheckpoint | undefined;
  compact(sessionId: string, options?: CompactOptions): CompactionResult;
  getCompactionState(sessionId: string): CompactionState;
  resetConsumerToCheckpoint(
    sessionId: string,
    consumerId: string,
    checkpointId: string,
  ): number;
  touchConsumerLease(
    sessionId: string,
    consumerId: string,
    ttlMs?: number,
  ): number;

  close(): void;
}

export interface ArchiveCheckpoint {
  checkpointId: string;
  sessionId: string;
  revision: number;
  archiveHash: string;
  prevArchiveHash?: string;
  recordCount: number;
  createdAt: number;
}

export interface RegisterCheckpointInput {
  checkpointId?: string;
  revision: number;
  archiveHash: string;
  prevArchiveHash?: string;
  recordCount: number;
}

export interface CompactOptions {
  checkpointId?: string;
  dryRun?: boolean;
}

export interface CompactionStats {
  revisionsRemoved: number;
  eventsRemoved: number;
  correctionsRemoved: number;
  leasesRemoved: number;
}

export interface CompactionResult {
  sessionId: string;
  compacted: boolean;
  skipped?:
    | "no-checkpoint"
    | "no-active-consumers"
    | "active-consumer-behind-checkpoint"
    | "nothing-to-compact";
  safeWatermark: number;
  checkpointRevision: number;
  checkpointId: string;
  before: CompactionState;
  after: CompactionState;
  removed: CompactionStats;
}

export interface CompactionState {
  sessionId: string;
  compactedThroughRevision: number;
  baselineRevision: number;
  lastCheckpointId?: string;
  lastCompactedAt?: number;
  totalRevisionsRemoved: number;
  totalEventsRemoved: number;
  totalCorrectionsRemoved: number;
  totalLeasesRemoved: number;
}

export interface RevisionConsumer {
  readonly sessionId: string;
  readonly consumerId: string;
  fetch(limit?: number): RevisionRecord[];
  acknowledge(revision: number): number;
  getCursor(): number;
  resetToCheckpoint(checkpointId: string): number;
  heartbeat(ttlMs?: number): number;
}
