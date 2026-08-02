export type EventType = 'partial' | 'final';

export type ChangeType = 'partial-updated' | 'final-committed';

export type IngestStatus = 'accepted' | 'duplicate' | 'ignored';

export interface IngestEvent {
  eventId: string;
  sourceId: string;
  sourceSeq: number;
  type: EventType;
  content: string;
}

export interface IngestResult {
  status: IngestStatus;
  revision: number | null;
  reason: string | null;
}

export interface FragmentData {
  sourceId: string;
  sourceSeq: number;
  type: EventType;
  content: string;
}

export interface SourceSnapshot {
  sourceId: string;
  fragments: FragmentData[];
}

export interface SessionSummary {
  sourceCount: number;
  fragmentCount: number;
  finalCount: number;
  partialCount: number;
  textLength: number;
}

export interface Snapshot {
  sessionId: string;
  revision: number;
  sources: SourceSnapshot[];
  text: string;
  summary: SessionSummary;
}

export interface RevisionData {
  sessionId: string;
  revision: number;
  eventId: string;
  sourceId: string;
  sourceSeq: number;
  changeType: ChangeType;
  content: string;
  snapshotText: string;
  summary: SessionSummary;
  createdAt: number;
}

export interface StoreOptions {
  dbPath: string;
  busyTimeoutMs?: number;
}

export interface IncomingEventRow {
  session_id: string;
  event_id: string;
  source_id: string;
  source_seq: number;
  event_type: EventType;
  content: string;
  content_hash: string;
  resulted_revision: number | null;
  received_at: number;
}

export interface FragmentRow {
  session_id: string;
  source_id: string;
  source_seq: number;
  event_type: EventType;
  content: string;
  event_id: string;
  updated_at: number;
}

export interface RevisionRow {
  session_id: string;
  revision: number;
  event_id: string;
  source_id: string;
  source_seq: number;
  change_type: ChangeType;
  content: string;
  snapshot_text: string;
  summary: string;
  created_at: number;
}

export interface CursorRow {
  session_id: string;
  consumer_id: string;
  cursor_revision: number;
  updated_at: number;
}
