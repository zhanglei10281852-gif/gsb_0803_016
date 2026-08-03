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
