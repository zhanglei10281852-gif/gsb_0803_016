export class ConflictError extends Error {
  readonly code = 'CONFLICT';
  constructor(message: string) {
    super(message);
    this.name = 'ConflictError';
  }
}

export class ValidationError extends Error {
  readonly code = 'VALIDATION';
  constructor(message: string) {
    super(message);
    this.name = 'ValidationError';
  }
}

/** Why a review (lease/correction) operation was rejected. */
export type ReviewConflictReason =
  /** Another reviewer holds a live lease on the segment. */
  | 'lease_held'
  /** No lease, or a different leaseId, for this segment. */
  | 'no_lease'
  /** The lease expired before submission. */
  | 'lease_expired'
  /** baseRevision is older than the segment's current version. */
  | 'stale_base';

/**
 * Distinguishable conflict for the human-review flow: competing lease
 * acquisition, expired leases and stale-base submissions all fail with this
 * error, with `reason` telling the cases apart.
 */
export class ReviewConflictError extends Error {
  readonly code = 'REVIEW_CONFLICT';
  readonly reason: ReviewConflictReason;
  constructor(reason: ReviewConflictReason, message: string) {
    super(message);
    this.name = 'ReviewConflictError';
    this.reason = reason;
  }
}

/**
 * A consumer tried to resume from a cursor that compaction has already
 * reclaimed. It must rebuild from the latest checkpoint archive and reset
 * its cursor (see resetConsumerToCheckpoint); nothing is skipped silently.
 */
export class ResetRequiredError extends Error {
  readonly code = 'RESET_REQUIRED';
  /** Smallest revision still present in the stream. */
  readonly firstAvailableRevision: number;
  /** Revision covered by the latest checkpoint, if one exists. */
  readonly checkpointRevision: number | null;
  constructor(firstAvailableRevision: number, checkpointRevision: number | null) {
    super(
      `cursor is behind compacted history; first available revision is ${firstAvailableRevision}` +
        (checkpointRevision === null ? '' : `, latest checkpoint covers ${checkpointRevision}`),
    );
    this.name = 'ResetRequiredError';
    this.firstAvailableRevision = firstAvailableRevision;
    this.checkpointRevision = checkpointRevision;
  }
}
