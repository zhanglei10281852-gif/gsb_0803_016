import Database from "better-sqlite3";
import { initializeSchema } from "./schema";
import {
  buildSnapshot,
  createSnapshotStatements,
  type SnapshotStatements,
} from "./snapshot";
import {
  EventConflictError,
  InvalidEventError,
  RecognitionStoreError,
  RevisionNotFoundError,
} from "./errors";
import type {
  IngestOutcome,
  NormalizedRecognitionEvent,
  RecognitionEventInput,
  RecognitionStore as RecognitionStoreLike,
  RevisionConsumer,
  RevisionRecord,
  SegmentKind,
  Snapshot,
} from "./types";

export interface RecognitionStoreOptions {
  filename: string;
  busyTimeout?: number;
}

interface EventRow {
  event_id: string;
  source_id: string;
  source_seq: number;
  kind: SegmentKind;
  text: string;
}

interface EventIdRow {
  event_id: string;
}

interface SequenceRow {
  final_seq: number;
  partial_seq: number;
}

interface RevisionRow {
  revision: number;
  event_id: string | null;
  source_id: string | null;
  source_seq: number | null;
  kind: SegmentKind | null;
  snapshot: string;
  created_at: number;
}

interface CursorRow {
  cursor: number;
}

interface NextRevisionRow {
  revision: number;
}

interface MaxRevisionRow {
  max_revision: number;
}

const SqliteError = Database.SqliteError;

function isBusyError(error: unknown): error is Database.SqliteError {
  return (
    error instanceof SqliteError &&
    (error.code === "SQLITE_BUSY" ||
      error.code === "SQLITE_BUSY_SNAPSHOT" ||
      error.code === "SQLITE_LOCKED")
  );
}

function withBusyRetry<T>(operation: () => T, attempts = 30): T {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      return operation();
    } catch (error) {
      if (!isBusyError(error) || attempt === attempts - 1) {
        throw error;
      }
      const waitMs = 5 + Math.floor(Math.random() * 20);
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, waitMs);
    }
  }
  return operation();
}

function assertNonEmptyString(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new InvalidEventError(`${name} must be a non-empty string.`);
  }
  return value;
}

function normalizeEvent(input: RecognitionEventInput): NormalizedRecognitionEvent {
  if (input === null || typeof input !== "object") {
    throw new InvalidEventError("Event must be an object.");
  }

  const eventId = assertNonEmptyString(input.eventId, "eventId");
  const sourceId = assertNonEmptyString(input.sourceId, "sourceId");
  const text = typeof input.text === "string" ? input.text : null;

  if (text === null) {
    throw new InvalidEventError("text must be a string.");
  }
  if (typeof input.isFinal !== "boolean") {
    throw new InvalidEventError("isFinal must be a boolean.");
  }
  if (!Number.isSafeInteger(input.sourceSeq) || input.sourceSeq < 0) {
    throw new InvalidEventError("sourceSeq must be a non-negative safe integer.");
  }

  return {
    eventId,
    sourceId,
    sourceSeq: input.sourceSeq,
    kind: input.isFinal ? "final" : "partial",
    text,
  };
}

function canonicalizeEvents(
  events: readonly RecognitionEventInput[]
): NormalizedRecognitionEvent[] {
  return events
    .map(normalizeEvent)
    .sort((a, b) => {
      if (a.sourceId < b.sourceId) return -1;
      if (a.sourceId > b.sourceId) return 1;
      if (a.sourceSeq !== b.sourceSeq) return a.sourceSeq - b.sourceSeq;
      if (a.kind < b.kind) return -1;
      if (a.kind > b.kind) return 1;
      if (a.eventId < b.eventId) return -1;
      if (a.eventId > b.eventId) return 1;
      return 0;
    });
}

function sameEvent(row: EventRow, event: NormalizedRecognitionEvent): boolean {
  return (
    row.source_id === event.sourceId &&
    row.source_seq === event.sourceSeq &&
    row.kind === event.kind &&
    row.text === event.text
  );
}

function mapRevision(
  sessionId: string,
  row: RevisionRow
): RevisionRecord {
  return {
    sessionId,
    revision: row.revision,
    eventId: row.event_id ?? undefined,
    sourceId: row.source_id ?? undefined,
    sourceSeq: row.source_seq ?? undefined,
    kind: row.kind ?? undefined,
    snapshot: JSON.parse(row.snapshot) as Snapshot,
    createdAt: row.created_at,
  };
}

class SQLiteRevisionConsumer implements RevisionConsumer {
  constructor(
    private readonly store: RecognitionStore,
    public readonly sessionId: string,
    public readonly consumerId: string
  ) {}

  fetch(limit?: number): RevisionRecord[] {
    return this.store.fetchChanges(this.sessionId, this.consumerId, limit);
  }

  acknowledge(revision: number): number {
    return this.store.acknowledge(this.sessionId, this.consumerId, revision);
  }

  getCursor(): number {
    return this.store.getCursor(this.sessionId, this.consumerId);
  }
}

export class RecognitionStore implements RecognitionStoreLike {
  private readonly db: Database.Database;
  private readonly snapshots: SnapshotStatements;
  private readonly ingestTransaction: Database.Transaction<
    (sessionId: string, events: NormalizedRecognitionEvent[]) => IngestOutcome[]
  >;
  private readonly readTransaction: Database.Transaction<
    <T>(read: () => T) => T
  >;
  private readonly acknowledgeTransaction: Database.Transaction<
    (sessionId: string, consumerId: string, revision: number) => void
  >;

  constructor(options: RecognitionStoreOptions) {
    const filename = assertNonEmptyString(options.filename, "filename");
    const busyTimeout = Number.isSafeInteger(options.busyTimeout)
      ? (options.busyTimeout as number)
      : 10000;

    this.db = new Database(filename, { timeout: busyTimeout });
    this.db.pragma(`busy_timeout = ${busyTimeout}`);

    if (filename === ":memory:") {
      this.db.pragma("journal_mode = MEMORY");
      this.db.pragma("synchronous = NORMAL");
    } else {
      this.db.pragma("journal_mode = WAL");
      this.db.pragma("synchronous = FULL");
    }
    this.db.pragma("foreign_keys = ON");

    initializeSchema(this.db);
    this.snapshots = createSnapshotStatements(this.db);

    const getEvent = this.db.prepare<[string, string], EventRow>(
      `SELECT event_id, source_id, source_seq, kind, text
       FROM events WHERE session_id = ? AND event_id = ?`
    );
    const samePosition = this.db.prepare<
      [string, string, SegmentKind, number],
      EventIdRow
    >(
      `SELECT event_id FROM events
       WHERE session_id = ? AND source_id = ? AND kind = ? AND source_seq = ?`
    );
    const lastSeqs = this.db.prepare<[string, string], SequenceRow>(
      `SELECT
         COALESCE(MAX(CASE WHEN kind = 'final' THEN source_seq END), -1) AS final_seq,
         COALESCE(MAX(CASE WHEN kind = 'partial' THEN source_seq END), -1) AS partial_seq
       FROM events WHERE session_id = ? AND source_id = ?`
    );
    const insertEvent = this.db.prepare(
      `INSERT INTO events(session_id, event_id, source_id, source_seq, kind, text, created_at)
       VALUES(?, ?, ?, ?, ?, ?, ?)`
    );
    const revisionForEvent = this.db.prepare<[string, string], CursorRow>(
      `SELECT revision AS cursor FROM revisions WHERE session_id = ? AND event_id = ?`
    );
    const nextRevisionForSession = this.db.prepare<[string], NextRevisionRow>(
      `SELECT COALESCE(MAX(revision), 0) + 1 AS revision
       FROM revisions WHERE session_id = ?`
    );
    const insertRevision = this.db.prepare(
      `INSERT INTO revisions(
         session_id, revision, event_id, source_id, source_seq, kind, snapshot, created_at
       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`
    );

    this.ingestTransaction = this.db.transaction(
      (sessionId: string, events: NormalizedRecognitionEvent[]) => {
        const outcomes: IngestOutcome[] = [];

        for (const event of events) {
          const existing = getEvent.get(sessionId, event.eventId);
          if (existing) {
            if (!sameEvent(existing, event)) {
              throw new EventConflictError(
                event.eventId,
                "event-content",
                `Event ${event.eventId} was already recorded with different content.`
              );
            }
            const revision = revisionForEvent.get(
              sessionId,
              event.eventId
            )?.cursor;
            outcomes.push({
              status: "duplicate",
              eventId: event.eventId,
              ...(revision === undefined ? {} : { revision }),
            });
            continue;
          }

          const occupied = samePosition.get(
            sessionId,
            event.sourceId,
            event.kind,
            event.sourceSeq
          );
          if (occupied) {
            throw new EventConflictError(
              event.eventId,
              "source-sequence",
              `${event.kind} sequence ${event.sourceSeq} for source ${event.sourceId} is already used by event ${occupied.event_id}.`
            );
          }

          const seqs = lastSeqs.get(sessionId, event.sourceId);
          const finalSeq = seqs?.final_seq ?? -1;
          const partialSeq = seqs?.partial_seq ?? -1;
          const effective =
            event.kind === "final"
              ? true
              : event.sourceSeq > finalSeq &&
                event.sourceSeq > partialSeq;

          const now = Date.now();
          insertEvent.run(
            sessionId,
            event.eventId,
            event.sourceId,
            event.sourceSeq,
            event.kind,
            event.text,
            now
          );

          if (!effective) {
            outcomes.push({
              status: "stale",
              eventId: event.eventId,
            });
            continue;
          }

          const nextRow = nextRevisionForSession.get(sessionId);
          const revision = nextRow?.revision ?? 1;
          const snapshot = buildSnapshot(this.snapshots, sessionId);
          const completedSnapshot: Snapshot = {
            ...snapshot,
            revision,
            summary: { ...snapshot.summary, revision },
          };

          insertRevision.run(
            sessionId,
            revision,
            event.eventId,
            event.sourceId,
            event.sourceSeq,
            event.kind,
            JSON.stringify(completedSnapshot),
            now
          );

          outcomes.push({
            status: "inserted",
            eventId: event.eventId,
            revision,
          });
        }

        return outcomes;
      }
    );

    const revisionById = this.db.prepare<[string, number], RevisionRow>(
      `SELECT revision, event_id, source_id, source_seq, kind, snapshot, created_at
       FROM revisions WHERE session_id = ? AND revision = ?`
    );
    const changes = this.db.prepare<[string, number, number], RevisionRow>(
      `SELECT revision, event_id, source_id, source_seq, kind, snapshot, created_at
       FROM revisions
       WHERE session_id = ? AND revision > ?
       ORDER BY revision ASC
       LIMIT ?`
    );
    const cursor = this.db.prepare<[string, string], CursorRow>(
      `SELECT cursor FROM consumer_cursors WHERE session_id = ? AND consumer_id = ?`
    );
    const maxRevision = this.db.prepare<[string], MaxRevisionRow>(
      `SELECT COALESCE(MAX(revision), 0) AS max_revision
       FROM revisions WHERE session_id = ?`
    );
    const upsertCursor = this.db.prepare(
      `INSERT INTO consumer_cursors(session_id, consumer_id, cursor, updated_at)
       VALUES(?, ?, ?, ?)
       ON CONFLICT(session_id, consumer_id) DO UPDATE SET
         cursor = excluded.cursor,
         updated_at = excluded.updated_at
       WHERE consumer_cursors.cursor < excluded.cursor`
    );

    this.readTransaction = this.db.transaction(<T>(read: () => T) => read());
    this.acknowledgeTransaction = this.db.transaction(
      (sessionId: string, consumerId: string, revision: number) => {
        const max = maxRevision.get(sessionId)?.max_revision ?? 0;
        if (revision > max) {
          throw new RevisionNotFoundError(sessionId, revision);
        }
        upsertCursor.run(sessionId, consumerId, revision, Date.now());
      }
    );

    this.getRevisionStatement = revisionById;
    this.changesStatement = changes;
    this.cursorStatement = cursor;
  }

  private readonly getRevisionStatement: Database.Statement<
    [string, number],
    RevisionRow
  >;
  private readonly changesStatement: Database.Statement<
    [string, number, number],
    RevisionRow
  >;
  private readonly cursorStatement: Database.Statement<
    [string, string],
    CursorRow
  >;

  ingest(
    sessionId: string,
    event: RecognitionEventInput | readonly RecognitionEventInput[]
  ): IngestOutcome | IngestOutcome[] {
    assertNonEmptyString(sessionId, "sessionId");
    const list = Array.isArray(event)
      ? canonicalizeEvents(event)
      : [normalizeEvent(event as RecognitionEventInput)];
    const outcomes = withBusyRetry(() =>
      this.ingestTransaction.immediate(sessionId, list)
    );
    return Array.isArray(event) ? outcomes : (outcomes[0] as IngestOutcome);
  }

  getSnapshot(sessionId: string): Snapshot {
    assertNonEmptyString(sessionId, "sessionId");
    return withBusyRetry(
      () =>
        this.readTransaction.deferred(() =>
          buildSnapshot(this.snapshots, sessionId)
        ) as Snapshot
    );
  }

  getSnapshotAt(sessionId: string, revision: number): Snapshot {
    assertNonEmptyString(sessionId, "sessionId");
    if (!Number.isSafeInteger(revision) || revision < 0) {
      throw new RecognitionStoreError("revision must be a non-negative safe integer.");
    }
    if (revision === 0) {
      return {
        sessionId,
        revision: 0,
        segments: [],
        text: "",
        finalText: "",
        summary: {
          revision: 0,
          eventCount: 0,
          sourceCount: 0,
          finalSegmentCount: 0,
          activePartialCount: 0,
          textLength: 0,
          finalTextLength: 0,
        },
      };
    }
    return this.getRevision(sessionId, revision).snapshot;
  }

  getRevision(sessionId: string, revision: number): RevisionRecord {
    assertNonEmptyString(sessionId, "sessionId");
    if (!Number.isSafeInteger(revision) || revision <= 0) {
      throw new RevisionNotFoundError(sessionId, revision);
    }

    return withBusyRetry(
      () =>
        this.readTransaction.deferred(() => {
          const row = this.getRevisionStatement.get(sessionId, revision);
          if (!row) {
            throw new RevisionNotFoundError(sessionId, revision);
          }
          return mapRevision(sessionId, row);
        }) as RevisionRecord
    );
  }

  fetchChanges(
    sessionId: string,
    consumerId: string,
    limit = 100
  ): RevisionRecord[] {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(consumerId, "consumerId");
    if (!Number.isSafeInteger(limit) || limit <= 0) {
      throw new RecognitionStoreError("limit must be a positive safe integer.");
    }

    return withBusyRetry(
      () =>
        this.readTransaction.deferred(() => {
          const current =
            this.cursorStatement.get(sessionId, consumerId)?.cursor ?? 0;
          const rows = this.changesStatement.all(sessionId, current, limit);
          return rows.map((row) => mapRevision(sessionId, row));
        }) as RevisionRecord[]
    );
  }

  acknowledge(sessionId: string, consumerId: string, revision: number): number {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(consumerId, "consumerId");
    if (!Number.isSafeInteger(revision) || revision < 0) {
      throw new RecognitionStoreError("revision must be a non-negative safe integer.");
    }

    withBusyRetry(() => {
      this.acknowledgeTransaction.immediate(sessionId, consumerId, revision);
    });
    return this.getCursor(sessionId, consumerId);
  }

  getCursor(sessionId: string, consumerId: string): number {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(consumerId, "consumerId");
    return this.cursorStatement.get(sessionId, consumerId)?.cursor ?? 0;
  }

  createConsumer(sessionId: string, consumerId: string): RevisionConsumer {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(consumerId, "consumerId");
    return new SQLiteRevisionConsumer(this, sessionId, consumerId);
  }

  close(): void {
    this.db.close();
  }
}

export function createRecognitionStore(
  options: RecognitionStoreOptions
): RecognitionStore {
  return new RecognitionStore(options);
}
