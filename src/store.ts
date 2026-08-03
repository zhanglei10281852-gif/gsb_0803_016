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
  type ExportArchiveOptions,
  type ImportSessionArchiveOptions,
} from "./archive";
import type {
  ArchiveImportResult,
  SessionArchiveStream,
} from "./archive-types";
import {
  ConsumerResetRequiredError,
  EventConflictError,
  InvalidEventError,
  RecognitionStoreError,
  ReviewConflictError,
  RevisionCompactedError,
  RevisionNotFoundError,
} from "./errors";
import type {
  ArchiveCheckpoint,
  ChangeType,
  ClaimLeaseInput,
  ClaimLeaseOutcome,
  CompactOptions,
  CompactionResult,
  CompactionState,
  CompactionStats,
  CorrectionRecord,
  IngestOutcome,
  NormalizedRecognitionEvent,
  RecognitionEventInput,
  RecognitionStore as RecognitionStoreLike,
  RegisterCheckpointInput,
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
  lease_ttl_ms: number;
  last_seen_at: number;
  reset_to_checkpoint: string | null;
}

interface ConsumerRow {
  consumer_id: string;
  cursor: number;
  lease_ttl_ms: number;
  last_seen_at: number;
  reset_to_checkpoint: string | null;
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

  resetToCheckpoint(checkpointId: string): number {
    return this.store.resetConsumerToCheckpoint(
      this.sessionId,
      this.consumerId,
      checkpointId,
    );
  }

  heartbeat(ttlMs?: number): number {
    return this.store.touchConsumerLease(
      this.sessionId,
      this.consumerId,
      ttlMs,
    );
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
    const nextRevisionForSession = this.db.prepare<
      [string, string],
      NextRevisionRow
    >(
      `SELECT COALESCE(MAX(revision),
        (SELECT baseline_revision FROM compaction_state WHERE session_id = ?),
        0
      ) + 1 AS revision
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

          const nextRow = nextRevisionForSession.get(sessionId, sessionId);
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
    const currentMaxRevision = this.db.prepare<
      [string, string],
      MaxRevisionRow
    >(
      `SELECT COALESCE(MAX(revision),
        (SELECT baseline_revision FROM compaction_state WHERE session_id = ?),
        0
      ) AS max_revision
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
          currentMaxRevision.get(sessionId, sessionId)?.max_revision ?? 0;
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

        const nextRow = nextRevisionForSession.get(sessionId, sessionId);
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
      `SELECT cursor, lease_ttl_ms, last_seen_at, reset_to_checkpoint
       FROM consumer_cursors WHERE session_id = ? AND consumer_id = ?`,
    );
    const touchConsumer = this.db.prepare(
      `UPDATE consumer_cursors
       SET last_seen_at = ?, lease_ttl_ms = ?
       WHERE session_id = ? AND consumer_id = ?`,
    );
    const ensureConsumer = this.db.prepare(
      `INSERT INTO consumer_cursors(
         session_id, consumer_id, cursor, updated_at, lease_ttl_ms, last_seen_at
       ) VALUES(?, ?, 0, ?, ?, ?)
       ON CONFLICT(session_id, consumer_id) DO NOTHING`,
    );
    const activeConsumers = this.db.prepare<[string, number], ConsumerRow>(
      `SELECT consumer_id, cursor, lease_ttl_ms, last_seen_at, reset_to_checkpoint
       FROM consumer_cursors
       WHERE session_id = ? AND last_seen_at + lease_ttl_ms > ?`,
    );
    const resetCursorToCheckpoint = this.db.prepare(
      `UPDATE consumer_cursors
       SET cursor = ?, reset_to_checkpoint = ?, updated_at = ?, last_seen_at = ?
       WHERE session_id = ? AND consumer_id = ?`,
    );
    const maxRevision = this.db.prepare<[string, string], MaxRevisionRow>(
      `SELECT COALESCE(MAX(revision),
        (SELECT baseline_revision FROM compaction_state WHERE session_id = ?),
        0
      ) AS max_revision
       FROM revisions WHERE session_id = ?`,
    );
    const upsertCursor = this.db.prepare(
      `INSERT INTO consumer_cursors(
         session_id, consumer_id, cursor, updated_at, lease_ttl_ms, last_seen_at
       ) VALUES(?, ?, ?, ?, ?, ?)
       ON CONFLICT(session_id, consumer_id) DO UPDATE SET
         cursor = excluded.cursor,
         updated_at = excluded.updated_at
       WHERE consumer_cursors.cursor < excluded.cursor`,
    );

    const insertCheckpoint = this.db.prepare(
      `INSERT OR IGNORE INTO archive_checkpoints(
         session_id, checkpoint_id, revision, archive_hash, prev_archive_hash,
         record_count, created_at
       ) VALUES(?, ?, ?, ?, ?, ?, ?)`,
    );
    const checkpointById = this.db.prepare<
      [string, string],
      {
        checkpoint_id: string;
        revision: number;
        archive_hash: string;
        prev_archive_hash: string | null;
        record_count: number;
        created_at: number;
      }
    >(
      `SELECT checkpoint_id, revision, archive_hash, prev_archive_hash,
              record_count, created_at
       FROM archive_checkpoints WHERE session_id = ? AND checkpoint_id = ?`,
    );
    const latestCheckpoint = this.db.prepare<
      [string],
      {
        checkpoint_id: string;
        revision: number;
        archive_hash: string;
        prev_archive_hash: string | null;
        record_count: number;
        created_at: number;
      }
    >(
      `SELECT checkpoint_id, revision, archive_hash, prev_archive_hash,
              record_count, created_at
       FROM archive_checkpoints WHERE session_id = ?
       ORDER BY revision DESC, created_at DESC, checkpoint_id DESC LIMIT 1`,
    );
    const allCheckpoints = this.db.prepare<
      [string],
      {
        checkpoint_id: string;
        revision: number;
        archive_hash: string;
        prev_archive_hash: string | null;
        record_count: number;
        created_at: number;
      }
    >(
      `SELECT checkpoint_id, revision, archive_hash, prev_archive_hash,
              record_count, created_at
       FROM archive_checkpoints WHERE session_id = ?
       ORDER BY revision ASC, created_at ASC, checkpoint_id ASC`,
    );
    const compactionStateRow = this.db.prepare<
      [string],
      {
        compacted_through_revision: number;
        baseline_revision: number;
        last_checkpoint_id: string | null;
        last_compacted_at: number | null;
        total_revisions_removed: number;
        total_events_removed: number;
        total_corrections_removed: number;
        total_leases_removed: number;
      }
    >(
      `SELECT compacted_through_revision, baseline_revision, last_checkpoint_id,
              last_compacted_at, total_revisions_removed, total_events_removed,
              total_corrections_removed, total_leases_removed
       FROM compaction_state WHERE session_id = ?`,
    );
    const deleteRevisionsBefore = this.db.prepare(
      `DELETE FROM revisions WHERE session_id = ? AND revision <= ?`,
    );
    const deleteStaleEventsBefore = this.db.prepare(
      `DELETE FROM events
       WHERE session_id = ? AND kind = 'partial'
         AND EXISTS (
           SELECT 1 FROM revisions r
           WHERE r.session_id = events.session_id
             AND r.event_id = events.event_id
             AND r.revision <= ?
         )
         AND NOT EXISTS (
           SELECT 1 FROM revisions r2
           WHERE r2.session_id = events.session_id
             AND r2.event_id = events.event_id
             AND r2.revision > ?
         )
         AND EXISTS (
           SELECT 1 FROM events later
           WHERE later.session_id = events.session_id
             AND later.source_id = events.source_id
             AND (
               later.source_seq > events.source_seq
               OR (later.source_seq = events.source_seq AND later.kind = 'final')
             )
         )`,
    );
    const deleteSupersededCorrectionsBefore = this.db.prepare(
      `DELETE FROM corrections WHERE 0`,
    );
    const deleteReleasedLeasesBefore = this.db.prepare(
      `DELETE FROM review_leases
       WHERE session_id = ? AND released_at IS NOT NULL AND released_at <= ?
         AND NOT EXISTS (
           SELECT 1 FROM corrections c
           WHERE c.session_id = review_leases.session_id
             AND c.lease_id = review_leases.lease_id
         )`,
    );
    const upsertCompactionState = this.db.prepare(
      `INSERT INTO compaction_state(
         session_id, compacted_through_revision, baseline_revision,
         last_checkpoint_id, last_compacted_at, total_revisions_removed,
         total_events_removed, total_corrections_removed, total_leases_removed
       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(session_id) DO UPDATE SET
         compacted_through_revision = excluded.compacted_through_revision,
         baseline_revision = excluded.baseline_revision,
         last_checkpoint_id = excluded.last_checkpoint_id,
         last_compacted_at = excluded.last_compacted_at,
         total_revisions_removed = compaction_state.total_revisions_removed + excluded.total_revisions_removed,
         total_events_removed = compaction_state.total_events_removed + excluded.total_events_removed,
         total_corrections_removed = compaction_state.total_corrections_removed + excluded.total_corrections_removed,
         total_leases_removed = compaction_state.total_leases_removed + excluded.total_leases_removed`,
    );
    const countRevisionsBefore = this.db.prepare<
      [string, number],
      { c: number }
    >(
      `SELECT COUNT(*) AS c FROM revisions WHERE session_id = ? AND revision <= ?`,
    );
    const countStaleEventsBefore = this.db.prepare<
      [string, number, number],
      { c: number }
    >(
      `SELECT COUNT(*) AS c FROM events
       WHERE session_id = ? AND kind = 'partial'
         AND EXISTS (
           SELECT 1 FROM revisions r
           WHERE r.session_id = events.session_id
             AND r.event_id = events.event_id
             AND r.revision <= ?
         )
         AND NOT EXISTS (
           SELECT 1 FROM revisions r2
           WHERE r2.session_id = events.session_id
             AND r2.event_id = events.event_id
             AND r2.revision > ?
         )
         AND EXISTS (
           SELECT 1 FROM events later
           WHERE later.session_id = events.session_id
             AND later.source_id = events.source_id
             AND (
               later.source_seq > events.source_seq
               OR (later.source_seq = events.source_seq AND later.kind = 'final')
             )
         )`,
    );
    const countSupersededCorrectionsBefore = this.db.prepare<[], { c: number }>(
      `SELECT 0 AS c`,
    );
    const countReleasedLeasesBefore = this.db.prepare<
      [string, number],
      { c: number }
    >(
      `SELECT COUNT(*) AS c FROM review_leases
       WHERE session_id = ? AND released_at IS NOT NULL AND released_at <= ?
         AND NOT EXISTS (
           SELECT 1 FROM corrections c
           WHERE c.session_id = review_leases.session_id
             AND c.lease_id = review_leases.lease_id
         )`,
    );

    this.readTransaction = this.db.transaction(<T>(read: () => T) => read());
    this.acknowledgeTransaction = this.db.transaction(
      (sessionId: string, consumerId: string, revision: number) => {
        const max = maxRevision.get(sessionId, sessionId)?.max_revision ?? 0;
        if (revision > max) {
          throw new RevisionNotFoundError(sessionId, revision);
        }
        const now = this.clock();
        const existing = cursor.get(sessionId, consumerId);
        const ttl = existing?.lease_ttl_ms ?? 86400000;
        ensureConsumer.run(sessionId, consumerId, now, ttl, now);
        upsertCursor.run(sessionId, consumerId, revision, now, ttl, now);
      },
    );

    this.getRevisionStatement = revisionById;
    this.changesStatement = changes;
    this.cursorStatement = cursor;
    this.leaseByIdStatement = leaseById;
    this.leaseBySegmentStatement = leaseBySegment;
    this.correctionByIdStatement = correctionById;
    this.correctionsBySegmentStatement = correctionsBySegment;
    this.touchConsumerStatement = touchConsumer;
    this.ensureConsumerStatement = ensureConsumer;
    this.activeConsumersStatement = activeConsumers;
    this.resetCursorToCheckpointStatement = resetCursorToCheckpoint;
    this.insertCheckpointStatement = insertCheckpoint;
    this.checkpointByIdStatement = checkpointById;
    this.latestCheckpointStatement = latestCheckpoint;
    this.allCheckpointsStatement = allCheckpoints;
    this.compactionStateStatement = compactionStateRow;
    this.deleteRevisionsBeforeStatement = deleteRevisionsBefore;
    this.deleteStaleEventsBeforeStatement = deleteStaleEventsBefore;
    this.deleteSupersededCorrectionsBeforeStatement =
      deleteSupersededCorrectionsBefore;
    this.deleteReleasedLeasesBeforeStatement = deleteReleasedLeasesBefore;
    this.upsertCompactionStateStatement = upsertCompactionState;
    this.countRevisionsBeforeStatement = countRevisionsBefore;
    this.countStaleEventsBeforeStatement = countStaleEventsBefore;
    this.countSupersededCorrectionsBeforeStatement =
      countSupersededCorrectionsBefore;
    this.countReleasedLeasesBeforeStatement = countReleasedLeasesBefore;
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
  private readonly touchConsumerStatement: Database.Statement<
    [number, number, string, string],
    unknown
  >;
  private readonly ensureConsumerStatement: Database.Statement<
    [string, string, number, number, number],
    unknown
  >;
  private readonly activeConsumersStatement: Database.Statement<
    [string, number],
    ConsumerRow
  >;
  private readonly resetCursorToCheckpointStatement: Database.Statement<
    [number, string | null, number, number, string, string],
    unknown
  >;
  private readonly insertCheckpointStatement: Database.Statement<
    [string, string, number, string, string | null, number, number],
    unknown
  >;
  private readonly checkpointByIdStatement: Database.Statement<
    [string, string],
    {
      checkpoint_id: string;
      revision: number;
      archive_hash: string;
      prev_archive_hash: string | null;
      record_count: number;
      created_at: number;
    }
  >;
  private readonly latestCheckpointStatement: Database.Statement<
    [string],
    {
      checkpoint_id: string;
      revision: number;
      archive_hash: string;
      prev_archive_hash: string | null;
      record_count: number;
      created_at: number;
    }
  >;
  private readonly allCheckpointsStatement: Database.Statement<
    [string],
    {
      checkpoint_id: string;
      revision: number;
      archive_hash: string;
      prev_archive_hash: string | null;
      record_count: number;
      created_at: number;
    }
  >;
  private readonly compactionStateStatement: Database.Statement<
    [string],
    {
      compacted_through_revision: number;
      baseline_revision: number;
      last_checkpoint_id: string | null;
      last_compacted_at: number | null;
      total_revisions_removed: number;
      total_events_removed: number;
      total_corrections_removed: number;
      total_leases_removed: number;
    }
  >;
  private readonly deleteRevisionsBeforeStatement: Database.Statement<
    [string, number],
    unknown
  >;
  private readonly deleteStaleEventsBeforeStatement: Database.Statement<
    [string, number, number],
    unknown
  >;
  private readonly deleteSupersededCorrectionsBeforeStatement: Database.Statement<
    [],
    unknown
  >;
  private readonly deleteReleasedLeasesBeforeStatement: Database.Statement<
    [string, number],
    unknown
  >;
  private readonly upsertCompactionStateStatement: Database.Statement<
    [
      string,
      number,
      number,
      string | null,
      number | null,
      number,
      number,
      number,
      number,
    ],
    unknown
  >;
  private readonly countRevisionsBeforeStatement: Database.Statement<
    [string, number],
    { c: number }
  >;
  private readonly countStaleEventsBeforeStatement: Database.Statement<
    [string, number, number],
    { c: number }
  >;
  private readonly countSupersededCorrectionsBeforeStatement: Database.Statement<
    [],
    { c: number }
  >;
  private readonly countReleasedLeasesBeforeStatement: Database.Statement<
    [string, number],
    { c: number }
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
            const state = this.compactionStateStatement.get(sessionId);
            if (state && revision <= state.compacted_through_revision) {
              const cp = state.last_checkpoint_id
                ? this.checkpointByIdStatement.get(
                    sessionId,
                    state.last_checkpoint_id,
                  )
                : undefined;
              throw new RevisionCompactedError(
                sessionId,
                revision,
                state.compacted_through_revision,
                state.baseline_revision,
                cp?.checkpoint_id,
              );
            }
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
        this.db
          .transaction(() => {
            const consumer = this.cursorStatement.get(sessionId, consumerId);
            const current = consumer?.cursor ?? 0;
            const state = this.compactionStateStatement.get(sessionId);
            if (state && current < state.baseline_revision) {
              const resetCheckpointId =
                consumer?.reset_to_checkpoint ?? state.last_checkpoint_id;
              const cp = resetCheckpointId
                ? this.checkpointByIdStatement.get(sessionId, resetCheckpointId)
                : undefined;
              if (cp) {
                throw new ConsumerResetRequiredError(
                  sessionId,
                  consumerId,
                  cp.checkpoint_id,
                  cp.revision,
                  cp.archive_hash,
                );
              }
            }
            const rows = this.changesStatement.all(sessionId, current, limit);
            if (consumer) {
              this.touchConsumerStatement.run(
                this.clock(),
                consumer.lease_ttl_ms,
                sessionId,
                consumerId,
              );
            }
            return rows.map((row) => mapRevision(sessionId, row));
          })
          .immediate() as RevisionRecord[],
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

  exportArchive(
    sessionId: string,
    options?: ExportArchiveOptions,
  ): SessionArchiveStream {
    assertNonEmptyString(sessionId, "sessionId");
    return exportSessionArchive(this.db, sessionId, this.clock, options);
  }

  async writeArchive(
    sessionId: string,
    outputPath: string,
    options?: ExportArchiveOptions,
  ): Promise<void> {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(outputPath, "outputPath");
    await writeSessionArchive(
      this.db,
      sessionId,
      outputPath,
      this.clock,
      options,
    );
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

  registerCheckpoint(
    sessionId: string,
    input: RegisterCheckpointInput,
  ): ArchiveCheckpoint {
    assertNonEmptyString(sessionId, "sessionId");
    if (!Number.isSafeInteger(input.revision) || input.revision <= 0) {
      throw new RecognitionStoreError(
        "checkpoint revision must be a positive safe integer.",
      );
    }
    assertNonEmptyString(input.archiveHash, "archiveHash");
    if (!Number.isSafeInteger(input.recordCount) || input.recordCount < 0) {
      throw new RecognitionStoreError(
        "checkpoint recordCount must be a non-negative safe integer.",
      );
    }
    const checkpointId =
      input.checkpointId && input.checkpointId.length > 0
        ? input.checkpointId
        : randomUUID();

    return withBusyRetry(() => {
      let existing:
        | {
            checkpoint_id: string;
            revision: number;
            archive_hash: string;
            prev_archive_hash: string | null;
            record_count: number;
            created_at: number;
          }
        | undefined;
      this.db
        .transaction(() => {
          existing = this.checkpointByIdStatement.get(sessionId, checkpointId);
          if (existing) return;
          this.insertCheckpointStatement.run(
            sessionId,
            checkpointId,
            input.revision,
            input.archiveHash,
            input.prevArchiveHash ?? null,
            input.recordCount,
            this.clock(),
          );
        })
        .immediate();
      return this.mapCheckpoint(
        this.checkpointByIdStatement.get(sessionId, checkpointId) as {
          checkpoint_id: string;
          revision: number;
          archive_hash: string;
          prev_archive_hash: string | null;
          record_count: number;
          created_at: number;
        },
        sessionId,
      );
    });
  }

  getCheckpoint(
    sessionId: string,
    checkpointId: string,
  ): ArchiveCheckpoint | undefined {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(checkpointId, "checkpointId");
    const row = this.checkpointByIdStatement.get(sessionId, checkpointId);
    return row ? this.mapCheckpoint(row, sessionId) : undefined;
  }

  getCheckpoints(sessionId: string): ArchiveCheckpoint[] {
    assertNonEmptyString(sessionId, "sessionId");
    return this.allCheckpointsStatement
      .all(sessionId)
      .map((row) => this.mapCheckpoint(row, sessionId));
  }

  getCompactionState(sessionId: string): CompactionState {
    assertNonEmptyString(sessionId, "sessionId");
    const row = this.compactionStateStatement.get(sessionId);
    return {
      sessionId,
      compactedThroughRevision: row?.compacted_through_revision ?? 0,
      baselineRevision: row?.baseline_revision ?? 0,
      ...(row?.last_checkpoint_id
        ? { lastCheckpointId: row.last_checkpoint_id }
        : {}),
      ...(row?.last_compacted_at
        ? { lastCompactedAt: row.last_compacted_at }
        : {}),
      totalRevisionsRemoved: row?.total_revisions_removed ?? 0,
      totalEventsRemoved: row?.total_events_removed ?? 0,
      totalCorrectionsRemoved: row?.total_corrections_removed ?? 0,
      totalLeasesRemoved: row?.total_leases_removed ?? 0,
    };
  }

  touchConsumerLease(
    sessionId: string,
    consumerId: string,
    ttlMs?: number,
  ): number {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(consumerId, "consumerId");
    if (ttlMs !== undefined) {
      if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0) {
        throw new RecognitionStoreError(
          "ttlMs must be a positive safe integer.",
        );
      }
    }
    return withBusyRetry(() => {
      const now = this.clock();
      const existing = this.cursorStatement.get(sessionId, consumerId);
      const ttl = ttlMs ?? existing?.lease_ttl_ms ?? 86400000;
      if (!existing) {
        this.ensureConsumerStatement.run(sessionId, consumerId, now, ttl, now);
      } else {
        this.touchConsumerStatement.run(now, ttl, sessionId, consumerId);
      }
      return now + ttl;
    });
  }

  resetConsumerToCheckpoint(
    sessionId: string,
    consumerId: string,
    checkpointId: string,
  ): number {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(consumerId, "consumerId");
    assertNonEmptyString(checkpointId, "checkpointId");
    return withBusyRetry(() => {
      let newCursor = 0;
      this.db
        .transaction(() => {
          const cp = this.checkpointByIdStatement.get(sessionId, checkpointId);
          if (!cp) {
            throw new RecognitionStoreError(
              `Checkpoint ${checkpointId} does not exist in session ${sessionId}.`,
            );
          }
          const now = this.clock();
          const existing = this.cursorStatement.get(sessionId, consumerId);
          const ttl = existing?.lease_ttl_ms ?? 86400000;
          this.ensureConsumerStatement.run(
            sessionId,
            consumerId,
            now,
            ttl,
            now,
          );
          newCursor = cp.revision;
          this.resetCursorToCheckpointStatement.run(
            cp.revision,
            null,
            now,
            now,
            sessionId,
            consumerId,
          );
        })
        .immediate();
      return newCursor;
    });
  }

  compact(sessionId: string, options: CompactOptions = {}): CompactionResult {
    assertNonEmptyString(sessionId, "sessionId");
    return withBusyRetry(() => {
      const before = this.getCompactionState(sessionId);
      let result!: CompactionResult;
      this.db
        .transaction(() => {
          const checkpointRow = options.checkpointId
            ? this.checkpointByIdStatement.get(sessionId, options.checkpointId)
            : this.latestCheckpointStatement.get(sessionId);

          if (!checkpointRow) {
            result = {
              sessionId,
              compacted: false,
              skipped: "no-checkpoint",
              safeWatermark: 0,
              checkpointRevision: 0,
              checkpointId: "",
              before,
              after: before,
              removed: {
                revisionsRemoved: 0,
                eventsRemoved: 0,
                correctionsRemoved: 0,
                leasesRemoved: 0,
              },
            };
            return;
          }

          const now = this.clock();
          const activeConsumers = this.activeConsumersStatement.all(
            sessionId,
            now,
          );

          let minCursor = checkpointRow.revision;
          for (const c of activeConsumers) {
            if (c.cursor < minCursor) minCursor = c.cursor;
          }

          const safeWatermark = Math.min(checkpointRow.revision, minCursor);

          if (safeWatermark <= before.compactedThroughRevision) {
            result = {
              sessionId,
              compacted: false,
              skipped: "nothing-to-compact",
              safeWatermark,
              checkpointRevision: checkpointRow.revision,
              checkpointId: checkpointRow.checkpoint_id,
              before,
              after: before,
              removed: {
                revisionsRemoved: 0,
                eventsRemoved: 0,
                correctionsRemoved: 0,
                leasesRemoved: 0,
              },
            };
            return;
          }

          const revisionsRemoved = (
            this.countRevisionsBeforeStatement.get(
              sessionId,
              safeWatermark,
            ) as { c: number }
          ).c;
          const eventsRemoved = (
            this.countStaleEventsBeforeStatement.get(
              sessionId,
              safeWatermark,
              safeWatermark,
            ) as { c: number }
          ).c;
          const correctionsRemoved = (
            this.countSupersededCorrectionsBeforeStatement.get() as {
              c: number;
            }
          ).c;
          const leasesRemoved = (
            this.countReleasedLeasesBeforeStatement.get(
              sessionId,
              checkpointRow.created_at,
            ) as { c: number }
          ).c;

          if (!options.dryRun) {
            this.deleteStaleEventsBeforeStatement.run(
              sessionId,
              safeWatermark,
              safeWatermark,
            );
            this.deleteSupersededCorrectionsBeforeStatement.run();
            this.deleteReleasedLeasesBeforeStatement.run(
              sessionId,
              checkpointRow.created_at,
            );
            this.deleteRevisionsBeforeStatement.run(sessionId, safeWatermark);

            this.upsertCompactionStateStatement.run(
              sessionId,
              safeWatermark,
              safeWatermark,
              checkpointRow.checkpoint_id,
              now,
              revisionsRemoved,
              eventsRemoved,
              correctionsRemoved,
              leasesRemoved,
            );
          }

          const after = options.dryRun
            ? before
            : this.getCompactionState(sessionId);

          result = {
            sessionId,
            compacted: !options.dryRun,
            safeWatermark,
            checkpointRevision: checkpointRow.revision,
            checkpointId: checkpointRow.checkpoint_id,
            before,
            after,
            removed: {
              revisionsRemoved,
              eventsRemoved,
              correctionsRemoved,
              leasesRemoved,
            },
          };
        })
        .immediate();
      return result;
    });
  }

  private mapCheckpoint(
    row: {
      checkpoint_id: string;
      revision: number;
      archive_hash: string;
      prev_archive_hash: string | null;
      record_count: number;
      created_at: number;
    },
    sessionId: string,
  ): ArchiveCheckpoint {
    return {
      sessionId,
      checkpointId: row.checkpoint_id,
      revision: row.revision,
      archiveHash: row.archive_hash,
      ...(row.prev_archive_hash === null
        ? {}
        : { prevArchiveHash: row.prev_archive_hash }),
      recordCount: row.record_count,
      createdAt: row.created_at,
    };
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
