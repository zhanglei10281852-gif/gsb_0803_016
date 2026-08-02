export { RecognitionStore } from './store';
export { Session } from './session';
export { Consumer } from './consumer';
export type { StreamOptions } from './consumer';
export {
  EventConflictError,
  SlotFinalizedError,
  StoreClosedError,
  LeaseBusyError,
  LeaseExpiredError,
  LeaseConsumedError,
  LeaseNotFoundError,
  StaleBaseRevisionError,
} from './errors';
export type {
  IngestEvent,
  IngestResult,
  IngestStatus,
  EventType,
  ChangeType,
  FragmentData,
  SourceSnapshot,
  SessionSummary,
  Snapshot,
  RevisionData,
  CorrectionMetadata,
  StoreOptions,
  Lease,
  LeaseStatus,
  Correction,
  AcquireLeaseOptions,
  SubmitCorrectionOptions,
} from './types';
