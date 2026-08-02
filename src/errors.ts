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
