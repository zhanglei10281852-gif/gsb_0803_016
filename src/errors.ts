export class RecognitionStoreError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RecognitionStoreError";
  }
}

export class InvalidEventError extends RecognitionStoreError {
  constructor(message: string) {
    super(message);
    this.name = "InvalidEventError";
  }
}

export type EventConflictReason = "event-content" | "source-sequence";

export class EventConflictError extends RecognitionStoreError {
  public readonly eventId: string;
  public readonly reason: EventConflictReason;

  constructor(eventId: string, reason: EventConflictReason, message: string) {
    super(message);
    this.name = "EventConflictError";
    this.eventId = eventId;
    this.reason = reason;
  }
}

export class RevisionNotFoundError extends RecognitionStoreError {
  public readonly revision: number;

  constructor(sessionId: string, revision: number) {
    super(`Revision ${revision} does not exist for session ${sessionId}.`);
    this.name = "RevisionNotFoundError";
    this.revision = revision;
  }
}

export type ReviewConflictReason =
  | "lease-taken"
  | "lease-expired"
  | "lease-not-found"
  | "lease-actor-mismatch"
  | "base-revision-stale"
  | "segment-not-found";

export class ReviewConflictError extends RecognitionStoreError {
  public readonly reason: ReviewConflictReason;
  public readonly sourceId?: string;
  public readonly sourceSeq?: number;
  public readonly leaseId?: string;
  public readonly expectedRevision?: number;
  public readonly actualRevision?: number;

  constructor(
    reason: ReviewConflictReason,
    message: string,
    details?: {
      sourceId?: string;
      sourceSeq?: number;
      leaseId?: string;
      expectedRevision?: number;
      actualRevision?: number;
    },
  ) {
    super(message);
    this.name = "ReviewConflictError";
    this.reason = reason;
    this.sourceId = details?.sourceId;
    this.sourceSeq = details?.sourceSeq;
    this.leaseId = details?.leaseId;
    this.expectedRevision = details?.expectedRevision;
    this.actualRevision = details?.actualRevision;
  }
}

export class RevisionCompactedError extends RecognitionStoreError {
  public readonly sessionId: string;
  public readonly requestedRevision: number;
  public readonly compactedThroughRevision: number;
  public readonly baselineRevision: number;
  public readonly checkpointId?: string;

  constructor(
    sessionId: string,
    requestedRevision: number,
    compactedThroughRevision: number,
    baselineRevision: number,
    checkpointId?: string,
  ) {
    super(
      `Revision ${requestedRevision} in session ${sessionId} has been compacted; the earliest available revision is ${baselineRevision}.`,
    );
    this.name = "RevisionCompactedError";
    this.sessionId = sessionId;
    this.requestedRevision = requestedRevision;
    this.compactedThroughRevision = compactedThroughRevision;
    this.baselineRevision = baselineRevision;
    this.checkpointId = checkpointId;
  }
}

export class ConsumerResetRequiredError extends RecognitionStoreError {
  public readonly sessionId: string;
  public readonly consumerId: string;
  public readonly checkpointId: string;
  public readonly checkpointRevision: number;
  public readonly archiveHash: string;

  constructor(
    sessionId: string,
    consumerId: string,
    checkpointId: string,
    checkpointRevision: number,
    archiveHash: string,
  ) {
    super(
      `Consumer ${consumerId} in session ${sessionId} must reset to checkpoint ${checkpointId} at revision ${checkpointRevision} before resuming.`,
    );
    this.name = "ConsumerResetRequiredError";
    this.sessionId = sessionId;
    this.consumerId = consumerId;
    this.checkpointId = checkpointId;
    this.checkpointRevision = checkpointRevision;
    this.archiveHash = archiveHash;
  }
}

export type CompactionSkipReason =
  | "no-checkpoint"
  | "no-active-consumers"
  | "active-consumer-behind-checkpoint"
  | "nothing-to-compact";

export class CompactionSkippedError extends RecognitionStoreError {
  public readonly reason: CompactionSkipReason;

  constructor(reason: CompactionSkipReason, message: string) {
    super(message);
    this.name = "CompactionSkippedError";
    this.reason = reason;
  }
}

