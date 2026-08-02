/** Error codes surfaced by the hub, useful for programmatic handling. */
export type HubErrorCode =
  | "CONFLICT"
  | "VALIDATION"
  | "UNKNOWN_CONSUMER"
  | "STORE"
  /** A competing lease claim lost: the segment is actively leased by someone else. */
  | "LEASE_CONFLICT"
  /** A correction was submitted against a lease whose TTL had elapsed. */
  | "LEASE_EXPIRED"
  /** No active lease matched the (leaseId, actor) presented at submit time. */
  | "NO_LEASE"
  /** The segment moved past the lease's baseRevision before the correction landed. */
  | "STALE_BASE";

export class HubError extends Error {
  readonly code: HubErrorCode;
  readonly details?: Record<string, unknown>;
  constructor(code: HubErrorCode, message: string, details?: Record<string, unknown>) {
    super(message);
    this.name = "HubError";
    this.code = code;
    this.details = details;
  }
}

/**
 * Thrown when the same `eventId` is re-delivered with content that differs
 * from what was first recorded. This is never accepted (not even as an
 * update): a stable id must map to stable content.
 */
export class ConflictError extends HubError {
  constructor(message: string, details?: Record<string, unknown>) {
    super("CONFLICT", message, details);
    this.name = "ConflictError";
  }
}

/** Thrown for malformed events (missing ids, bad seq, etc.). */
export class ValidationError extends HubError {
  constructor(message: string, details?: Record<string, unknown>) {
    super("VALIDATION", message, details);
    this.name = "ValidationError";
  }
}

/**
 * Thrown when a lease acquisition loses a race: another actor already holds a
 * live (non-expired) lease on the same segment. Distinguishable from the
 * post-submit failures below so callers can retry acquisition vs. re-fetch.
 */
export class LeaseConflictError extends HubError {
  constructor(message: string, details?: Record<string, unknown>) {
    super("LEASE_CONFLICT", message, details);
    this.name = "LeaseConflictError";
  }
}

/** Thrown when a correction is submitted after its lease TTL elapsed. */
export class LeaseExpiredError extends HubError {
  constructor(message: string, details?: Record<string, unknown>) {
    super("LEASE_EXPIRED", message, details);
    this.name = "LeaseExpiredError";
  }
}

/** Thrown when no active lease matches the (leaseId, actor) at submit time. */
export class NoLeaseError extends HubError {
  constructor(message: string, details?: Record<string, unknown>) {
    super("NO_LEASE", message, details);
    this.name = "NoLeaseError";
  }
}

/**
 * Thrown when a correction's lease was taken against a baseRevision that no
 * longer reflects the segment's current revision (a newer change landed first).
 */
export class StaleBaseError extends HubError {
  constructor(message: string, details?: Record<string, unknown>) {
    super("STALE_BASE", message, details);
    this.name = "StaleBaseError";
  }
}
