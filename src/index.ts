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
  ArchiveFormatError,
  ArchiveChecksumError,
  ArchiveVersionError,
  SessionExistsError,
  ConsumerResetRequiredError,
  NoCheckpointError,
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
  CompactionStats,
  Checkpoint,
  ConsumerLeaseInfo,
  ConsumerOptions,
} from './types';
export type {
  ArchiveCounts,
  ExportResult,
  ImportResult,
} from './archive';
