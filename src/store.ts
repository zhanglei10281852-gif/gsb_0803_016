import Database from 'better-sqlite3';
import { buildSnapshot, eventHash, type EventRow } from './core';
import { ConflictError, ValidationError } from './errors';
import { SCHEMA } from './schema';
import type {
  AsrEvent,
  ChangeRecord,
  IngestResult,
  PollResult,
  RevisionEntry,
  Snapshot,
} from './types';

export interface StoreOptions {
  /** How long a writer waits for the SQLite write lock. Default 10s. */
  busyTimeoutMs?: number;
}

interface SessionRow {
  last_revision: number;
}

interface RevisionRow {
  revision: number;
  change_json: string;
}

const MAX_LIMIT = 10_000;

function validateId(name: string, value: unknown): asserts value is string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new ValidationError(`${name} must be a non-empty string`);
  }
}

function validateEvent(event: AsrEvent): void {
  if (event === null || typeof event !== 'object') {
    throw new ValidationError('event must be an object');
  }
  validateId('event.eventId', event.eventId);
  if (!Number.isInteger(event.sourceSeq) || event.sourceSeq < 0) {
    throw new ValidationError('event.sourceSeq must be a non-negative integer');
  }
  if (event.kind !== 'partial' && event.kind !== 'final') {
    throw new ValidationError("event.kind must be 'partial' or 'final'");
  }
  if (typeof event.text !== 'string') {
    throw new ValidationError('event.text must be a string');
  }
  if (event.startMs !== undefined && (!Number.isFinite(event.startMs) || event.startMs < 0)) {
    throw new ValidationError('event.startMs must be a non-negative finite number');
  }
}

/**
 * Durable transcript store for one SQLite database file.
 *
 * Multiple TranscriptStore instances (same process or different processes)
 * may share one file; every state change is a single IMMEDIATE transaction,
 * so a crash can never leave half-applied results.
 */
export class TranscriptStore {
  private readonly db: Database.Database;
  private readonly txIngestOne: Database.Transaction<(s: string, src: string, e: AsrEvent) => IngestResult>;
  private readonly txIngestMany: Database.Transaction<(s: string, src: string, e: AsrEvent[]) => IngestResult[]>;

  /** `file` is a SQLite path, or ':memory:' for an ephemeral store. */
  constructor(file: string, options: StoreOptions = {}) {
    this.db = new Database(file);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = FULL');
    this.db.pragma(`busy_timeout = ${Math.max(0, options.busyTimeoutMs ?? 10_000)}`);
    this.db.exec(SCHEMA);
    this.txIngestOne = this.db.transaction((s: string, src: string, e: AsrEvent) =>
      this.applyOne(s, src, e),
    );
    this.txIngestMany = this.db.transaction((s: string, src: string, events: AsrEvent[]) =>
      events.map((e) => this.applyOne(s, src, e)),
    );
  }

  /**
   * Ingest one event. Idempotent for exact duplicates, throws ConflictError
   * when the eventId already exists with different content, and silently
   * drops events whose sourceSeq is not newer than the source's high-water
   * mark (covers both old partials and partials arriving after a final).
   */
  ingest(sessionId: string, sourceId: string, event: AsrEvent): IngestResult {
    return this.txIngestOne.immediate(sessionId, sourceId, event);
  }

  /**
   * Ingest a batch atomically: either every event is applied or none is
   * (e.g. when one of them conflicts). Results are per-event, in order.
   */
  ingestBatch(sessionId: string, sourceId: string, events: AsrEvent[]): IngestResult[] {
    return this.txIngestMany.immediate(sessionId, sourceId, events);
  }

  /** Deterministic point-in-time view of a session. */
  snapshot(sessionId: string): Snapshot {
    validateId('sessionId', sessionId);
    const row = this.db
      .prepare('SELECT last_revision FROM sessions WHERE session_id = ?')
      .get(sessionId) as SessionRow | undefined;
    const events = this.db
      .prepare(
        'SELECT source_id, event_id, source_seq, kind, text, start_ms FROM events WHERE session_id = ?',
      )
      .all(sessionId) as EventRow[];
    return buildSnapshot(sessionId, row?.last_revision ?? 0, events);
  }

  /** Last revision assigned within a session (0 if the session is empty/unknown). */
  lastRevision(sessionId: string): number {
    validateId('sessionId', sessionId);
    const row = this.db
      .prepare('SELECT last_revision FROM sessions WHERE session_id = ?')
      .get(sessionId) as SessionRow | undefined;
    return row?.last_revision ?? 0;
  }

  /**
   * Read up to `limit` revisions after the consumer's acked cursor.
   * Entries are delivered in strictly increasing revision order. Anything
   * not yet acked may be delivered again; acked revisions never are.
   */
  poll(consumerId: string, sessionId: string, limit = 100): PollResult {
    validateId('consumerId', consumerId);
    validateId('sessionId', sessionId);
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIMIT) {
      throw new ValidationError(`limit must be an integer in [1, ${MAX_LIMIT}]`);
    }
    const read = this.db.transaction((): PollResult => {
      const last = this.lastRevision(sessionId);
      const acked = this.cursor(consumerId, sessionId);
      const rows = this.db
        .prepare(
          'SELECT revision, change_json FROM revisions WHERE session_id = ? AND revision > ? ORDER BY revision LIMIT ?',
        )
        .all(sessionId, acked, limit) as RevisionRow[];
      const entries: RevisionEntry[] = rows.map((r) => ({
        revision: r.revision,
        change: JSON.parse(r.change_json) as ChangeRecord,
      }));
      return { entries, ackedRevision: acked, lastRevision: last };
    });
    return read();
  }

  /**
   * Advance the consumer's cursor to `revision` (clamped to the session's
   * last revision). Acks are monotonic: acknowledging an older revision is
   * a no-op. Returns the effective cursor after the call.
   */
  ack(consumerId: string, sessionId: string, revision: number): number {
    validateId('consumerId', consumerId);
    validateId('sessionId', sessionId);
    if (!Number.isInteger(revision) || revision < 0) {
      throw new ValidationError('revision must be a non-negative integer');
    }
    const write = this.db.transaction((): number => {
      const last = this.lastRevision(sessionId);
      const target = Math.min(revision, last);
      this.db
        .prepare(
          `INSERT INTO cursors (consumer_id, session_id, acked_revision) VALUES (?, ?, ?)
           ON CONFLICT (consumer_id, session_id)
           DO UPDATE SET acked_revision = MAX(acked_revision, excluded.acked_revision)`,
        )
        .run(consumerId, sessionId, target);
      return this.cursor(consumerId, sessionId);
    });
    return write.immediate();
  }

  /** Current acked cursor for a consumer (0 if it never acked). */
  cursor(consumerId: string, sessionId: string): number {
    validateId('consumerId', consumerId);
    validateId('sessionId', sessionId);
    const row = this.db
      .prepare('SELECT acked_revision FROM cursors WHERE consumer_id = ? AND session_id = ?')
      .get(consumerId, sessionId) as { acked_revision: number } | undefined;
    return row?.acked_revision ?? 0;
  }

  close(): void {
    this.db.close();
  }

  // --------------------------------------------------------------------

  private applyOne(sessionId: string, sourceId: string, event: AsrEvent): IngestResult {
    validateId('sessionId', sessionId);
    validateId('sourceId', sourceId);
    validateEvent(event);

    this.db.prepare('INSERT OR IGNORE INTO sessions (session_id) VALUES (?)').run(sessionId);
    const session = this.db
      .prepare('SELECT last_revision FROM sessions WHERE session_id = ?')
      .get(sessionId) as SessionRow;

    const existing = this.db
      .prepare('SELECT content_hash FROM events WHERE session_id = ? AND source_id = ? AND event_id = ?')
      .get(sessionId, sourceId, event.eventId) as { content_hash: string } | undefined;
    if (existing) {
      if (existing.content_hash === eventHash(event)) {
        return { status: 'duplicate', revision: session.last_revision };
      }
      throw new ConflictError(
        `event "${event.eventId}" from source "${sourceId}" already exists with different content`,
      );
    }

    const highWater = this.db
      .prepare(
        `SELECT MAX(CASE WHEN kind = 'partial' THEN source_seq END) AS mp,
                MAX(CASE WHEN kind = 'final' THEN source_seq END) AS mf
         FROM events WHERE session_id = ? AND source_id = ?`,
      )
      .get(sessionId, sourceId) as { mp: number | null; mf: number | null };
    const maxPartial = highWater.mp ?? -1;
    const maxFinal = highWater.mf ?? -1;

    if (event.kind === 'partial') {
      // Stale when superseded by a newer partial, or when it belongs to an
      // utterance whose final is already confirmed (cannot roll back).
      if (event.sourceSeq <= maxPartial || event.sourceSeq <= maxFinal) {
        return { status: 'stale', revision: session.last_revision };
      }
      // A newer partial supersedes the previous tentative partial.
      this.db
        .prepare("DELETE FROM events WHERE session_id = ? AND source_id = ? AND kind = 'partial'")
        .run(sessionId, sourceId);
    } else {
      // Finals always commit (even when reordered behind a newer partial),
      // and clear tentative partials of their own or earlier utterances.
      this.db
        .prepare(
          "DELETE FROM events WHERE session_id = ? AND source_id = ? AND kind = 'partial' AND source_seq <= ?",
        )
        .run(sessionId, sourceId, event.sourceSeq);
    }

    const revision = session.last_revision + 1;
    this.db
      .prepare(
        `INSERT INTO events (session_id, source_id, event_id, source_seq, kind, text, start_ms, content_hash)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        sessionId,
        sourceId,
        event.eventId,
        event.sourceSeq,
        event.kind,
        event.text,
        event.startMs ?? null,
        eventHash(event),
      );
    this.db
      .prepare('UPDATE sessions SET last_revision = ? WHERE session_id = ?')
      .run(revision, sessionId);
    const change: ChangeRecord = {
      type: event.kind,
      sourceId,
      eventId: event.eventId,
      sourceSeq: event.sourceSeq,
      text: event.text,
      startMs: event.startMs ?? null,
    };
    this.db
      .prepare('INSERT INTO revisions (session_id, revision, change_json, created_at) VALUES (?, ?, ?, ?)')
      .run(sessionId, revision, JSON.stringify(change), Date.now());

    return { status: 'applied', revision };
  }
}
