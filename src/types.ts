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
    event: RecognitionEventInput | readonly RecognitionEventInput[]
  ): IngestOutcome | IngestOutcome[];
  getSnapshot(sessionId: string): Snapshot;
  getSnapshotAt(sessionId: string, revision: number): Snapshot;
  getRevision(sessionId: string, revision: number): RevisionRecord;
  fetchChanges(
    sessionId: string,
    consumerId: string,
    limit?: number
  ): RevisionRecord[];
  acknowledge(sessionId: string, consumerId: string, revision: number): number;
  getCursor(sessionId: string, consumerId: string): number;
  createConsumer(sessionId: string, consumerId: string): RevisionConsumer;

  claimLease(
    sessionId: string,
    input: ClaimLeaseInput
  ): ClaimLeaseOutcome;
  releaseLease(sessionId: string, leaseId: string, actor: string): void;
  getLease(sessionId: string, leaseId: string): ReviewLease | undefined;
  getActiveLease(
    sessionId: string,
    sourceId: string,
    sourceSeq: number
  ): ReviewLease | undefined;
  submitCorrection(
    sessionId: string,
    input: SubmitCorrectionInput
  ): SubmitCorrectionOutcome;
  getCorrection(
    sessionId: string,
    correctionId: string
  ): CorrectionRecord | undefined;
  getCorrectionLineage(
    sessionId: string,
    sourceId: string,
    sourceSeq: number
  ): CorrectionRecord[];

  close(): void;
}

export interface RevisionConsumer {
  readonly sessionId: string;
  readonly consumerId: string;
  fetch(limit?: number): RevisionRecord[];
  acknowledge(revision: number): number;
  getCursor(): number;
}
