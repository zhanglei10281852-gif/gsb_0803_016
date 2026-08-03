import Database from "better-sqlite3";
import { randomUUID } from "node:crypto";
import type { Readable } from "node:stream";
import { initializeSchema } from "./schema";
import {
  buildSnapshot,
  createSnapshotStatements,
  type SnapshotStatements,
} from "./snapshot";
import {
  exportSessionArchive,
  importSessionArchive,
  importSessionArchiveFile,
  writeSessionArchive,
  type ImportSessionArchiveOptions,
} from "./archive";
import type {
  ArchiveImportResult,
  SessionArchiveStream,
} from "./archive-types";
import {
  EventConflictError,
  InvalidEventError,
  RecognitionStoreError,
  ReviewConflictError,
  RevisionNotFoundError,
} from "./errors";
import type {
  ChangeType,
  ClaimLeaseInput,
  ClaimLeaseOutcome,
  CorrectionRecord,
  IngestOutcome,
  NormalizedRecognitionEvent,
  RecognitionEventInput,
  RecognitionStore as RecognitionStoreLike,
  ReviewLease,
  RevisionConsumer,
  RevisionRecord,
  SegmentKind,
  Snapshot,
  SubmitCorrectionInput,
  SubmitCorrectionOutcome,
} from "./types";

export interface RecognitionStoreOptions {
  filename: string;
  busyTimeout?: number;
  now?: () => number;
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
  change_type: ChangeType;
  event_id: string | null;
  source_id: string | null;
  source_seq: number | null;
  kind: SegmentKind | null;
  correction_id: string | null;
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

interface LeaseRow {
  lease_id: string;
  source_id: string;
  source_seq: number;
  actor: string;
  base_revision: number;
  ttl_ms: number;
  claimed_at: number;
  expires_at: number;
  released_at: number | null;
}

interface SegmentTextRow {
  text: string;
}

interface CorrectionExistsRow {
  correction_id: string;
}

interface CorrectionRow {
  correction_id: string;
  source_id: string;
  source_seq: number;
  actor: string;
  reason: string;
  original_text: string;
  corrected_text: string;
  base_revision: number;
  supersedes: string | null;
  lease_id: string;
  created_at: number;
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

function withBusyRetry<T>(operation: () => T, attempts = 50): T {
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

function normalizeEvent(
  input: RecognitionEventInput,
): NormalizedRecognitionEvent {
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
    throw new InvalidEventError(
      "sourceSeq must be a non-negative safe integer.",
    );
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
  events: readonly RecognitionEventInput[],
): NormalizedRecognitionEvent[] {
  return events.map(normalizeEvent).sort((a, b) => {
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

function mapLease(sessionId: string, row: LeaseRow, now: number): ReviewLease {
  const active = row.released_at === null && row.expires_at > now;
  return {
    sessionId,
    leaseId: row.lease_id,
    sourceId: row.source_id,
    sourceSeq: row.source_seq,
    actor: row.actor,
    baseRevision: row.base_revision,
    ttlMs: row.ttl_ms,
    claimedAt: row.claimed_at,
    expiresAt: row.expires_at,
    ...(row.released_at === null ? {} : { releasedAt: row.released_at }),
    active,
  };
}

function mapCorrection(
  sessionId: string,
  row: CorrectionRow,
): CorrectionRecord {
  return {
    sessionId,
    correctionId: row.correction_id,
    sourceId: row.source_id,
    sourceSeq: row.source_seq,
    actor: row.actor,
    reason: row.reason,
    originalText: row.original_text,
    correctedText: row.corrected_text,
    baseRevision: row.base_revision,
    ...(row.supersedes === null ? {} : { supersedes: row.supersedes }),
    leaseId: row.lease_id,
    createdAt: row.created_at,
  };
}

function mapRevision(sessionId: string, row: RevisionRow): RevisionRecord {
  return {
    sessionId,
    revision: row.revision,
    changeType: row.change_type,
    eventId: row.event_id ?? undefined,
    sourceId: row.source_id ?? undefined,
    sourceSeq: row.source_seq ?? undefined,
    kind: row.kind ?? undefined,
    correctionId: row.correction_id ?? undefined,
    snapshot: JSON.parse(row.snapshot) as Snapshot,
    createdAt: row.created_at,
  };
}

class SQLiteRevisionConsumer implements RevisionConsumer {
  constructor(
    private readonly store: RecognitionStore,
    public readonly sessionId: string,
    public readonly consumerId: string,
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
  private readonly clock: () => number;
  private readonly ingestTransaction: Database.Transaction<
    (sessionId: string, events: NormalizedRecognitionEvent[]) => IngestOutcome[]
  >;
  private readonly claimLeaseTransaction: Database.Transaction<
    (
      sessionId: string,
      input: ClaimLeaseInput,
      leaseId: string,
      now: number,
    ) => ClaimLeaseOutcome
  >;
  private readonly submitCorrectionTransaction: Database.Transaction<
    (
      sessionId: string,
      input: SubmitCorrectionInput,
      correctionId: string,
      now: number,
    ) => SubmitCorrectionOutcome
  >;
  private readonly releaseLeaseTransaction: Database.Transaction<
    (sessionId: string, leaseId: string, actor: string, now: number) => void
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
    this.clock = typeof options.now === "function" ? options.now : Date.now;

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
       FROM events WHERE session_id = ? AND event_id = ?`,
    );
    const samePosition = this.db.prepare<
      [string, string, SegmentKind, number],
      EventIdRow
    >(
      `SELECT event_id FROM events
       WHERE session_id = ? AND source_id = ? AND kind = ? AND source_seq = ?`,
    );
    const lastSeqs = this.db.prepare<[string, string], SequenceRow>(
      `SELECT
         COALESCE(MAX(CASE WHEN kind = 'final' THEN source_seq END), -1) AS final_seq,
         COALESCE(MAX(CASE WHEN kind = 'partial' THEN source_seq END), -1) AS partial_seq
       FROM events WHERE session_id = ? AND source_id = ?`,
    );
    const insertEvent = this.db.prepare(
      `INSERT INTO events(session_id, event_id, source_id, source_seq, kind, text, created_at)
       VALUES(?, ?, ?, ?, ?, ?, ?)`,
    );
    const revisionForEvent = this.db.prepare<[string, string], CursorRow>(
      `SELECT revision AS cursor FROM revisions WHERE session_id = ? AND event_id = ?`,
    );
    const nextRevisionForSession = this.db.prepare<[string], NextRevisionRow>(
      `SELECT COALESCE(MAX(revision), 0) + 1 AS revision
       FROM revisions WHERE session_id = ?`,
    );
    const insertRevision = this.db.prepare(
      `INSERT INTO revisions(
         session_id, revision, change_type, event_id, source_id, source_seq,
         kind, correction_id, snapshot, created_at
       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
                `Event ${event.eventId} was already recorded with different content.`,
              );
            }
            const revision = revisionForEvent.get(
              sessionId,
              event.eventId,
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
            event.sourceSeq,
          );
          if (occupied) {
            throw new EventConflictError(
              event.eventId,
              "source-sequence",
              `${event.kind} sequence ${event.sourceSeq} for source ${event.sourceId} is already used by event ${occupied.event_id}.`,
            );
          }

          const seqs = lastSeqs.get(sessionId, event.sourceId);
          const finalSeq = seqs?.final_seq ?? -1;
          const partialSeq = seqs?.partial_seq ?? -1;
          const effective =
            event.kind === "final"
              ? true
              : event.sourceSeq > finalSeq && event.sourceSeq > partialSeq;

          const now = this.clock();
          insertEvent.run(
            sessionId,
            event.eventId,
            event.sourceId,
            event.sourceSeq,
            event.kind,
            event.text,
            now,
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
            "event",
            event.eventId,
            event.sourceId,
            event.sourceSeq,
            event.kind,
            null,
            JSON.stringify(completedSnapshot),
            now,
          );

          outcomes.push({
            status: "inserted",
            eventId: event.eventId,
            revision,
          });
        }

        return outcomes;
      },
    );

    const leaseBySegment = this.db.prepare<[string, string, number], LeaseRow>(
      `SELECT lease_id, source_id, source_seq, actor, base_revision, ttl_ms,
              claimed_at, expires_at, released_at
       FROM review_leases
       WHERE session_id = ? AND source_id = ? AND source_seq = ?`,
    );
    const leaseById = this.db.prepare<[string, string], LeaseRow>(
      `SELECT lease_id, source_id, source_seq, actor, base_revision, ttl_ms,
              claimed_at, expires_at, released_at
       FROM review_leases
       WHERE session_id = ? AND lease_id = ?`,
    );
    const insertLease = this.db.prepare(
      `INSERT INTO review_leases(
         session_id, source_id, source_seq, lease_id, actor, base_revision,
         ttl_ms, claimed_at, expires_at
       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    );
    const replaceLease = this.db.prepare(
      `UPDATE review_leases
       SET lease_id = ?, actor = ?, base_revision = ?, ttl_ms = ?,
           claimed_at = ?, expires_at = ?, released_at = NULL
       WHERE session_id = ? AND source_id = ? AND source_seq = ?`,
    );
    const releaseLeaseById = this.db.prepare(
      `UPDATE review_leases
       SET released_at = ?
       WHERE session_id = ? AND lease_id = ? AND released_at IS NULL`,
    );

    const segmentExists = this.db.prepare<[string, string, number], EventIdRow>(
      `SELECT event_id FROM events
       WHERE session_id = ? AND source_id = ? AND source_seq = ? AND kind = 'final'`,
    );
    const currentSegmentText = this.db.prepare<
      [string, string, number, string, string, number],
      SegmentTextRow
    >(
      `WITH base AS (
         SELECT text FROM events
         WHERE session_id = ? AND source_id = ? AND source_seq = ? AND kind = 'final'
       ), latest_correction AS (
         SELECT corrected_text
         FROM corrections
         WHERE session_id = ? AND source_id = ? AND source_seq = ?
         ORDER BY created_at DESC, correction_id DESC
         LIMIT 1
       )
       SELECT COALESCE(
         (SELECT corrected_text FROM latest_correction),
         (SELECT text FROM base)
       ) AS text`,
    );
    const currentMaxRevision = this.db.prepare<[string], MaxRevisionRow>(
      `SELECT COALESCE(MAX(revision), 0) AS max_revision
       FROM revisions WHERE session_id = ?`,
    );
    const previousCorrection = this.db.prepare<
      [string, string, number],
      CorrectionExistsRow
    >(
      `SELECT correction_id FROM corrections
       WHERE session_id = ? AND source_id = ? AND source_seq = ?
       ORDER BY created_at DESC, correction_id DESC
       LIMIT 1`,
    );
    const insertCorrection = this.db.prepare(
      `INSERT INTO corrections(
         session_id, correction_id, source_id, source_seq, actor, reason,
         original_text, corrected_text, base_revision, supersedes, lease_id, created_at
       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    );
    const correctionById = this.db.prepare<[string, string], CorrectionRow>(
      `SELECT correction_id, source_id, source_seq, actor, reason,
              original_text, corrected_text, base_revision, supersedes,
              lease_id, created_at
       FROM corrections WHERE session_id = ? AND correction_id = ?`,
    );
    const correctionsBySegment = this.db.prepare<
      [string, string, number],
      CorrectionRow
    >(
      `SELECT correction_id, source_id, source_seq, actor, reason,
              original_text, corrected_text, base_revision, supersedes,
              lease_id, created_at
       FROM corrections
       WHERE session_id = ? AND source_id = ? AND source_seq = ?
       ORDER BY created_at ASC, correction_id ASC`,
    );

    this.claimLeaseTransaction = this.db.transaction(
      (
        sessionId: string,
        input: ClaimLeaseInput,
        leaseId: string,
        now: number,
      ): ClaimLeaseOutcome => {
        if (!segmentExists.get(sessionId, input.sourceId, input.sourceSeq)) {
          throw new ReviewConflictError(
            "segment-not-found",
            `Final segment ${input.sourceId}:${input.sourceSeq} does not exist in session ${sessionId}.`,
            { sourceId: input.sourceId, sourceSeq: input.sourceSeq },
          );
        }

        const existingRow = leaseBySegment.get(
          sessionId,
          input.sourceId,
          input.sourceSeq,
        );
        const claimedAt = now;
        const expiresAt = now + input.ttlMs;

        if (!existingRow) {
          insertLease.run(
            sessionId,
            input.sourceId,
            input.sourceSeq,
            leaseId,
            input.actor,
            input.baseRevision,
            input.ttlMs,
            claimedAt,
            expiresAt,
          );
          const created = leaseBySegment.get(
            sessionId,
            input.sourceId,
            input.sourceSeq,
          ) as LeaseRow;
          return { lease: mapLease(sessionId, created, now), acquired: true };
        }

        const isActive =
          existingRow.released_at === null && existingRow.expires_at > now;
        const sameActor = existingRow.actor === input.actor;

        if (isActive && !sameActor) {
          throw new ReviewConflictError(
            "lease-taken",
            `Segment ${input.sourceId}:${input.sourceSeq} is already leased by ${existingRow.actor}.`,
            {
              sourceId: input.sourceId,
              sourceSeq: input.sourceSeq,
              leaseId: existingRow.lease_id,
            },
          );
        }

        if (isActive && sameActor) {
          return {
            lease: mapLease(sessionId, existingRow, now),
            acquired: false,
          };
        }

        replaceLease.run(
          leaseId,
          input.actor,
          input.baseRevision,
          input.ttlMs,
          claimedAt,
          expiresAt,
          sessionId,
          input.sourceId,
          input.sourceSeq,
        );
        const replaced = leaseBySegment.get(
          sessionId,
          input.sourceId,
          input.sourceSeq,
        ) as LeaseRow;
        return { lease: mapLease(sessionId, replaced, now), acquired: true };
      },
    );

    this.releaseLeaseTransaction = this.db.transaction(
      (sessionId: string, leaseId: string, actor: string, now: number) => {
        const row = leaseById.get(sessionId, leaseId);
        if (!row) {
          throw new ReviewConflictError(
            "lease-not-found",
            `Lease ${leaseId} does not exist.`,
            { leaseId },
          );
        }
        if (row.actor !== actor) {
          throw new ReviewConflictError(
            "lease-actor-mismatch",
            `Lease ${leaseId} belongs to ${row.actor}, not ${actor}.`,
            { leaseId },
          );
        }
        if (row.released_at !== null) {
          return;
        }
        if (row.expires_at <= now) {
          releaseLeaseById.run(now, sessionId, leaseId);
          return;
        }
        releaseLeaseById.run(now, sessionId, leaseId);
      },
    );

    this.submitCorrectionTransaction = this.db.transaction(
      (
        sessionId: string,
        input: SubmitCorrectionInput,
        correctionId: string,
        now: number,
      ): SubmitCorrectionOutcome => {
        const leaseRow = leaseById.get(sessionId, input.leaseId);
        if (!leaseRow) {
          throw new ReviewConflictError(
            "lease-not-found",
            `Lease ${input.leaseId} does not exist.`,
            { leaseId: input.leaseId },
          );
        }
        if (leaseRow.actor !== input.actor) {
          throw new ReviewConflictError(
            "lease-actor-mismatch",
            `Lease ${input.leaseId} belongs to ${leaseRow.actor}, not ${input.actor}.`,
            {
              leaseId: input.leaseId,
              sourceId: leaseRow.source_id,
              sourceSeq: leaseRow.source_seq,
            },
          );
        }
        if (leaseRow.released_at !== null || leaseRow.expires_at <= now) {
          throw new ReviewConflictError(
            "lease-expired",
            `Lease ${input.leaseId} is expired or released.`,
            {
              leaseId: input.leaseId,
              sourceId: leaseRow.source_id,
              sourceSeq: leaseRow.source_seq,
            },
          );
        }

        const currentRevision =
          currentMaxRevision.get(sessionId)?.max_revision ?? 0;
        if (input.baseRevision !== currentRevision) {
          throw new ReviewConflictError(
            "base-revision-stale",
            `Correction base revision ${input.baseRevision} does not match current revision ${currentRevision}.`,
            {
              sourceId: leaseRow.source_id,
              sourceSeq: leaseRow.source_seq,
              leaseId: input.leaseId,
              expectedRevision: input.baseRevision,
              actualRevision: currentRevision,
            },
          );
        }

        const textRow = currentSegmentText.get(
          sessionId,
          leaseRow.source_id,
          leaseRow.source_seq,
          sessionId,
          leaseRow.source_id,
          leaseRow.source_seq,
        );
        if (!textRow) {
          throw new ReviewConflictError(
            "segment-not-found",
            `Final segment ${leaseRow.source_id}:${leaseRow.source_seq} does not exist.`,
            { sourceId: leaseRow.source_id, sourceSeq: leaseRow.source_seq },
          );
        }

        const previous = previousCorrection.get(
          sessionId,
          leaseRow.source_id,
          leaseRow.source_seq,
        );
        const supersedes = previous?.correction_id;

        insertCorrection.run(
          sessionId,
          correctionId,
          leaseRow.source_id,
          leaseRow.source_seq,
          input.actor,
          input.reason,
          textRow.text,
          input.correctedText,
          input.baseRevision,
          supersedes ?? null,
          input.leaseId,
          now,
        );

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
          "correction",
          null,
          leaseRow.source_id,
          leaseRow.source_seq,
          "final",
          correctionId,
          JSON.stringify(completedSnapshot),
          now,
        );

        releaseLeaseById.run(now, sessionId, input.leaseId);

        const correctionRow = correctionById.get(
          sessionId,
          correctionId,
        ) as CorrectionRow;

        return {
          correction: mapCorrection(sessionId, correctionRow),
          revision,
        };
      },
    );

    const revisionById = this.db.prepare<[string, number], RevisionRow>(
      `SELECT revision, change_type, event_id, source_id, source_seq,
              kind, correction_id, snapshot, created_at
       FROM revisions WHERE session_id = ? AND revision = ?`,
    );
    const changes = this.db.prepare<[string, number, number], RevisionRow>(
      `SELECT revision, change_type, event_id, source_id, source_seq,
              kind, correction_id, snapshot, created_at
       FROM revisions
       WHERE session_id = ? AND revision > ?
       ORDER BY revision ASC
       LIMIT ?`,
    );
    const cursor = this.db.prepare<[string, string], CursorRow>(
      `SELECT cursor FROM consumer_cursors WHERE session_id = ? AND consumer_id = ?`,
    );
    const maxRevision = this.db.prepare<[string], MaxRevisionRow>(
      `SELECT COALESCE(MAX(revision), 0) AS max_revision
       FROM revisions WHERE session_id = ?`,
    );
    const upsertCursor = this.db.prepare(
      `INSERT INTO consumer_cursors(session_id, consumer_id, cursor, updated_at)
       VALUES(?, ?, ?, ?)
       ON CONFLICT(session_id, consumer_id) DO UPDATE SET
         cursor = excluded.cursor,
         updated_at = excluded.updated_at
       WHERE consumer_cursors.cursor < excluded.cursor`,
    );

    this.readTransaction = this.db.transaction(<T>(read: () => T) => read());
    this.acknowledgeTransaction = this.db.transaction(
      (sessionId: string, consumerId: string, revision: number) => {
        const max = maxRevision.get(sessionId)?.max_revision ?? 0;
        if (revision > max) {
          throw new RevisionNotFoundError(sessionId, revision);
        }
        upsertCursor.run(sessionId, consumerId, revision, this.clock());
      },
    );

    this.getRevisionStatement = revisionById;
    this.changesStatement = changes;
    this.cursorStatement = cursor;
    this.leaseByIdStatement = leaseById;
    this.leaseBySegmentStatement = leaseBySegment;
    this.correctionByIdStatement = correctionById;
    this.correctionsBySegmentStatement = correctionsBySegment;
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
  private readonly leaseByIdStatement: Database.Statement<
    [string, string],
    LeaseRow
  >;
  private readonly leaseBySegmentStatement: Database.Statement<
    [string, string, number],
    LeaseRow
  >;
  private readonly correctionByIdStatement: Database.Statement<
    [string, string],
    CorrectionRow
  >;
  private readonly correctionsBySegmentStatement: Database.Statement<
    [string, string, number],
    CorrectionRow
  >;

  ingest(
    sessionId: string,
    event: RecognitionEventInput | readonly RecognitionEventInput[],
  ): IngestOutcome | IngestOutcome[] {
    assertNonEmptyString(sessionId, "sessionId");
    const list = Array.isArray(event)
      ? canonicalizeEvents(event)
      : [normalizeEvent(event as RecognitionEventInput)];
    const outcomes = withBusyRetry(() =>
      this.ingestTransaction.immediate(sessionId, list),
    );
    return Array.isArray(event) ? outcomes : (outcomes[0] as IngestOutcome);
  }

  claimLease(sessionId: string, input: ClaimLeaseInput): ClaimLeaseOutcome {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(input.sourceId, "sourceId");
    assertNonEmptyString(input.actor, "actor");
    if (!Number.isSafeInteger(input.sourceSeq) || input.sourceSeq < 0) {
      throw new RecognitionStoreError(
        "sourceSeq must be a non-negative safe integer.",
      );
    }
    if (!Number.isSafeInteger(input.baseRevision) || input.baseRevision < 0) {
      throw new RecognitionStoreError(
        "baseRevision must be a non-negative safe integer.",
      );
    }
    if (!Number.isSafeInteger(input.ttlMs) || input.ttlMs <= 0) {
      throw new RecognitionStoreError("ttlMs must be a positive safe integer.");
    }

    const leaseId = randomUUID();
    return withBusyRetry(() =>
      this.claimLeaseTransaction.immediate(
        sessionId,
        input,
        leaseId,
        this.clock(),
      ),
    );
  }

  releaseLease(sessionId: string, leaseId: string, actor: string): void {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(leaseId, "leaseId");
    assertNonEmptyString(actor, "actor");
    withBusyRetry(() =>
      this.releaseLeaseTransaction.immediate(
        sessionId,
        leaseId,
        actor,
        this.clock(),
      ),
    );
  }

  getLease(sessionId: string, leaseId: string): ReviewLease | undefined {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(leaseId, "leaseId");
    return withBusyRetry(() =>
      this.readTransaction.deferred(() => {
        const row = this.leaseByIdStatement.get(sessionId, leaseId);
        return row ? mapLease(sessionId, row, this.clock()) : undefined;
      }),
    ) as ReviewLease | undefined;
  }

  getActiveLease(
    sessionId: string,
    sourceId: string,
    sourceSeq: number,
  ): ReviewLease | undefined {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(sourceId, "sourceId");
    return withBusyRetry(() =>
      this.readTransaction.deferred(() => {
        const row = this.leaseBySegmentStatement.get(
          sessionId,
          sourceId,
          sourceSeq,
        );
        if (!row) return undefined;
        const lease = mapLease(sessionId, row, this.clock());
        return lease.active ? lease : undefined;
      }),
    ) as ReviewLease | undefined;
  }

  submitCorrection(
    sessionId: string,
    input: SubmitCorrectionInput,
  ): SubmitCorrectionOutcome {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(input.leaseId, "leaseId");
    assertNonEmptyString(input.actor, "actor");
    assertNonEmptyString(input.reason, "reason");
    if (typeof input.correctedText !== "string") {
      throw new RecognitionStoreError("correctedText must be a string.");
    }
    if (!Number.isSafeInteger(input.baseRevision) || input.baseRevision < 0) {
      throw new RecognitionStoreError(
        "baseRevision must be a non-negative safe integer.",
      );
    }

    const correctionId = randomUUID();
    return withBusyRetry(() =>
      this.submitCorrectionTransaction.immediate(
        sessionId,
        input,
        correctionId,
        this.clock(),
      ),
    );
  }

  getCorrection(
    sessionId: string,
    correctionId: string,
  ): CorrectionRecord | undefined {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(correctionId, "correctionId");
    return withBusyRetry(() =>
      this.readTransaction.deferred(() => {
        const row = this.correctionByIdStatement.get(sessionId, correctionId);
        return row ? mapCorrection(sessionId, row) : undefined;
      }),
    ) as CorrectionRecord | undefined;
  }

  getCorrectionLineage(
    sessionId: string,
    sourceId: string,
    sourceSeq: number,
  ): CorrectionRecord[] {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(sourceId, "sourceId");
    return withBusyRetry(() =>
      this.readTransaction.deferred(() =>
        this.correctionsBySegmentStatement
          .all(sessionId, sourceId, sourceSeq)
          .map((row) => mapCorrection(sessionId, row)),
      ),
    ) as CorrectionRecord[];
  }

  getSnapshot(sessionId: string): Snapshot {
    assertNonEmptyString(sessionId, "sessionId");
    return withBusyRetry(
      () =>
        this.readTransaction.deferred(() =>
          buildSnapshot(this.snapshots, sessionId),
        ) as Snapshot,
    );
  }

  getSnapshotAt(sessionId: string, revision: number): Snapshot {
    assertNonEmptyString(sessionId, "sessionId");
    if (!Number.isSafeInteger(revision) || revision < 0) {
      throw new RecognitionStoreError(
        "revision must be a non-negative safe integer.",
      );
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
          correctedSegmentCount: 0,
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
        }) as RevisionRecord,
    );
  }

  fetchChanges(
    sessionId: string,
    consumerId: string,
    limit = 100,
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
        }) as RevisionRecord[],
    );
  }

  acknowledge(sessionId: string, consumerId: string, revision: number): number {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(consumerId, "consumerId");
    if (!Number.isSafeInteger(revision) || revision < 0) {
      throw new RecognitionStoreError(
        "revision must be a non-negative safe integer.",
      );
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

  exportArchive(sessionId: string): SessionArchiveStream {
    assertNonEmptyString(sessionId, "sessionId");
    return exportSessionArchive(this.db, sessionId, this.clock);
  }

  async writeArchive(sessionId: string, outputPath: string): Promise<void> {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(outputPath, "outputPath");
    await writeSessionArchive(this.db, sessionId, outputPath, this.clock);
  }

  async importArchive(
    sessionId: string,
    stream: Readable,
    options: ImportSessionArchiveOptions = {},
  ): Promise<ArchiveImportResult> {
    assertNonEmptyString(sessionId, "sessionId");
    return importSessionArchive(this.db, sessionId, stream, options);
  }

  async importArchiveFile(
    sessionId: string,
    inputPath: string,
    options: ImportSessionArchiveOptions = {},
  ): Promise<ArchiveImportResult> {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(inputPath, "inputPath");
    return importSessionArchiveFile(this.db, sessionId, inputPath, options);
  }

  close(): void {
    this.db.close();
  }
}

export function createRecognitionStore(
  options: RecognitionStoreOptions,
): RecognitionStore {
  return new RecognitionStore(options);
}
