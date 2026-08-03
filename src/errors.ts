export class EventConflictError extends Error {
  public readonly code = 'EVENT_CONFLICT';
  constructor(message: string) {
    super(message);
    this.name = 'EventConflictError';
  }
}

export class SlotFinalizedError extends Error {
  public readonly code = 'SLOT_FINALIZED';
  constructor(message: string) {
    super(message);
    this.name = 'SlotFinalizedError';
  }
}

export class StoreClosedError extends Error {
  public readonly code = 'STORE_CLOSED';
  constructor() {
    super('The recognition store has been closed.');
    this.name = 'StoreClosedError';
  }
}

export class LeaseBusyError extends Error {
  public readonly code = 'LEASE_BUSY';
  constructor(message: string) {
    super(message);
    this.name = 'LeaseBusyError';
  }
}

export class LeaseExpiredError extends Error {
  public readonly code = 'LEASE_EXPIRED';
  constructor(message: string) {
    super(message);
    this.name = 'LeaseExpiredError';
  }
}

export class LeaseConsumedError extends Error {
  public readonly code = 'LEASE_CONSUMED';
  constructor(message: string) {
    super(message);
    this.name = 'LeaseConsumedError';
  }
}

export class LeaseNotFoundError extends Error {
  public readonly code = 'LEASE_NOT_FOUND';
  constructor(message: string) {
    super(message);
    this.name = 'LeaseNotFoundError';
  }
}

export class StaleBaseRevisionError extends Error {
  public readonly code = 'STALE_BASE_REVISION';
  constructor(message: string) {
    super(message);
    this.name = 'StaleBaseRevisionError';
  }
}

export class ArchiveFormatError extends Error {
  public readonly code = 'ARCHIVE_FORMAT';
  constructor(message: string) {
    super(message);
    this.name = 'ArchiveFormatError';
  }
}

export class ArchiveChecksumError extends Error {
  public readonly code = 'ARCHIVE_CHECKSUM';
  constructor(message: string) {
    super(message);
    this.name = 'ArchiveChecksumError';
  }
}

export class ArchiveVersionError extends Error {
  public readonly code = 'ARCHIVE_VERSION';
  constructor(message: string) {
    super(message);
    this.name = 'ArchiveVersionError';
  }
}

export class SessionExistsError extends Error {
  public readonly code = 'SESSION_EXISTS';
  constructor(message: string) {
    super(message);
    this.name = 'SessionExistsError';
  }
}

export class ConsumerResetRequiredError extends Error {
  public readonly code = 'CONSUMER_RESET_REQUIRED';
  public readonly safeRevision: number;
  public readonly currentRevision: number;
  public readonly checkpointRevision: number;
  constructor(
    message: string,
    safeRevision: number,
    currentRevision: number,
    checkpointRevision: number,
  ) {
    super(message);
    this.name = 'ConsumerResetRequiredError';
    this.safeRevision = safeRevision;
    this.currentRevision = currentRevision;
    this.checkpointRevision = checkpointRevision;
  }
}

export class NoCheckpointError extends Error {
  public readonly code = 'NO_CHECKPOINT';
  constructor(message: string) {
    super(message);
    this.name = 'NoCheckpointError';
  }
}
