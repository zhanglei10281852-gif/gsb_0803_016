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
