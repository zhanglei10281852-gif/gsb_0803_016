export interface RecognitionEventInput {
  eventId: string;
  sourceId: string;
  sourceSeq: number;
  isFinal: boolean;
  text: string;
}

export type SegmentKind = "final" | "partial";

export interface NormalizedRecognitionEvent {
  eventId: string;
  sourceId: string;
  sourceSeq: number;
  kind: SegmentKind;
  text: string;
}

export interface Segment {
  sourceId: string;
  sourceSeq: number;
  kind: SegmentKind;
  text: string;
}

export interface SnapshotSummary {
  revision: number;
  eventCount: number;
  sourceCount: number;
  finalSegmentCount: number;
  activePartialCount: number;
  textLength: number;
  finalTextLength: number;
}

export interface Snapshot {
  sessionId: string;
  revision: number;
  segments: Segment[];
  text: string;
  finalText: string;
  summary: SnapshotSummary;
}

export type IngestStatus = "inserted" | "duplicate" | "stale";

export interface IngestOutcome {
  status: IngestStatus;
  eventId: string;
  revision?: number;
}

export interface RevisionRecord {
  sessionId: string;
  revision: number;
  eventId?: string;
  sourceId?: string;
  sourceSeq?: number;
  kind?: SegmentKind;
  snapshot: Snapshot;
  createdAt: number;
}

export interface RecognitionStore {
  ingest(
    sessionId: string,
    event: RecognitionEventInput | readonly RecognitionEventInput[]
  ): IngestOutcome | IngestOutcome[];
  getSnapshot(sessionId: string): Snapshot;
  getSnapshotAt(sessionId: string, revision: number): Snapshot;
  getRevision(sessionId: string, revision: number): RevisionRecord;
  fetchChanges(
    sessionId: string,
    consumerId: string,
    limit?: number
  ): RevisionRecord[];
  acknowledge(sessionId: string, consumerId: string, revision: number): number;
  getCursor(sessionId: string, consumerId: string): number;
  createConsumer(sessionId: string, consumerId: string): RevisionConsumer;
  close(): void;
}

export interface RevisionConsumer {
  readonly sessionId: string;
  readonly consumerId: string;
  fetch(limit?: number): RevisionRecord[];
  acknowledge(revision: number): number;
  getCursor(): number;
}
