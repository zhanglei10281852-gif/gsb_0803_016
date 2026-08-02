/** Error codes surfaced by the hub, useful for programmatic handling. */
export type HubErrorCode =
  | "CONFLICT"
  | "VALIDATION"
  | "UNKNOWN_CONSUMER"
  | "STORE";

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
