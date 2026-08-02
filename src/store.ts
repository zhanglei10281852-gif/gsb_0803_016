import Database from 'better-sqlite3';
import { randomUUID } from 'node:crypto';
import { buildSnapshot, correctionKey, eventHash, type EventRow } from './core';
import { ConflictError, ReviewConflictError, ValidationError } from './errors';
import { MIGRATIONS, SCHEMA } from './schema';
import type {
  AcquireLeaseOptions,
  AsrEvent,
  ChangeRecord,
  CorrectionInfo,
  IngestResult,
  LeaseInfo,
  PollResult,
  RevisionEntry,
  SegmentRef,
  Snapshot,
  SubmitCorrectionOptions,
  SubmitCorrectionResult,
} from './types';

export interface StoreOptions {
  /** How long a writer waits for the SQLite write lock. Default 10s. */
  busyTimeoutMs?: number;
  /** Clock override (ms epoch), mainly for tests. Defaults to Date.now. */
  now?: () => number;
}

interface SessionRow {
  last_revision: number;
}

interface RevisionRow {
  revision: number;
  change_json: string;
}

interface TargetRow {
  source_seq: number;
  kind: 'partial' | 'final';
  text: string;
  start_ms: number | null;
  applied_revision: number | null;
}

interface LeaseRow {
  lease_id: string;
  actor: string;
  base_revision: number;
  expires_at: number;
}

interface CorrectionRow {
  correction_id: string;
  target_source_id: string;
  target_event_id: string;
  text: string;
  actor: string;
  reason: string;
  supersedes: string | null;
  revision: number;
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
  private readonly nowFn: () => number;
  private readonly txIngestOne: Database.Transaction<(s: string, src: string, e: AsrEvent) => IngestResult>;
  private readonly txIngestMany: Database.Transaction<(s: string, src: string, e: AsrEvent[]) => IngestResult[]>;
  private readonly txAcquire: Database.Transaction<
    (s: string, t: SegmentRef, o: AcquireLeaseOptions) => LeaseInfo
  >;
  private readonly txSubmit: Database.Transaction<
    (s: string, t: SegmentRef, o: SubmitCorrectionOptions) => SubmitCorrectionResult
  >;

  /** `file` is a SQLite path, or ':memory:' for an ephemeral store. */
  constructor(file: string, options: StoreOptions = {}) {
    this.db = new Database(file);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = FULL');
    this.db.pragma(`busy_timeout = ${Math.max(0, options.busyTimeoutMs ?? 10_000)}`);
    this.nowFn = options.now ?? Date.now;
    this.db.exec(SCHEMA);
    this.migrate();
    this.txIngestOne = this.db.transaction((s: string, src: string, e: AsrEvent) =>
      this.applyOne(s, src, e),
    );
    this.txIngestMany = this.db.transaction((s: string, src: string, events: AsrEvent[]) =>
      events.map((e) => this.applyOne(s, src, e)),
    );
    this.txAcquire = this.db.transaction((s: string, t: SegmentRef, o: AcquireLeaseOptions) =>
      this.acquireOne(s, t, o),
    );
    this.txSubmit = this.db.transaction((s: string, t: SegmentRef, o: SubmitCorrectionOptions) =>
      this.submitOne(s, t, o),
    );
  }

  private migrate(): void {
    const cols = (this.db.pragma('table_info(events)') as { name: string }[]).map((c) => c.name);
    if (!cols.includes('applied_revision')) {
      for (const sql of MIGRATIONS) this.db.exec(sql);
    }
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

  /** Deterministic point-in-time view of a session (latest corrections overlaid). */
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
    return buildSnapshot(
      sessionId,
      row?.last_revision ?? 0,
      events,
      this.latestCorrections(sessionId),
    );
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

  // --------------------------- human review ---------------------------

  /**
   * Acquire a time-limited correction lease on a final segment. At most one
   * live lease exists per segment: a competing acquisition fails with
   * ReviewConflictError('lease_held') until the lease expires. Acquiring
   * with a baseRevision older than the segment's current version fails with
   * ReviewConflictError('stale_base').
   */
  acquireLease(sessionId: string, target: SegmentRef, options: AcquireLeaseOptions): LeaseInfo {
    return this.txAcquire.immediate(sessionId, target, options);
  }

  /**
   * Submit a correction under a live lease. The original event is never
   * rewritten; the correction records actor/reason and supersedes the
   * previous correction for the segment, and is appended to the session's
   * revision stream as type 'correction'. The lease is consumed. Expired
   * leases and stale bases fail with ReviewConflictError.
   */
  submitCorrection(
    sessionId: string,
    target: SegmentRef,
    options: SubmitCorrectionOptions,
  ): SubmitCorrectionResult {
    return this.txSubmit.immediate(sessionId, target, options);
  }

  /** Release a lease without submitting. Returns true if it was released. */
  releaseLease(sessionId: string, target: SegmentRef, leaseId: string): boolean {
    validateId('sessionId', sessionId);
    validateId('target.sourceId', target?.sourceId);
    validateId('target.eventId', target?.eventId);
    validateId('leaseId', leaseId);
    const res = this.db
      .prepare(
        'DELETE FROM leases WHERE session_id = ? AND target_source_id = ? AND target_event_id = ? AND lease_id = ?',
      )
      .run(sessionId, target.sourceId, target.eventId, leaseId);
    return res.changes > 0;
  }

  /** The live lease on a segment, or null (expired leases count as absent). */
  lease(sessionId: string, target: SegmentRef): (LeaseInfo & { actor: string; baseRevision: number }) | null {
    validateId('sessionId', sessionId);
    validateId('target.sourceId', target?.sourceId);
    validateId('target.eventId', target?.eventId);
    const row = this.db
      .prepare(
        'SELECT lease_id, actor, base_revision, expires_at FROM leases WHERE session_id = ? AND target_source_id = ? AND target_event_id = ?',
      )
      .get(sessionId, target.sourceId, target.eventId) as LeaseRow | undefined;
    if (!row || row.expires_at <= this.nowFn()) return null;
    return {
      leaseId: row.lease_id,
      actor: row.actor,
      baseRevision: row.base_revision,
      expiresAt: row.expires_at,
    };
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
        `INSERT INTO events (session_id, source_id, event_id, source_seq, kind, text, start_ms, content_hash, applied_revision)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
        revision,
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

  // ------------------------- review internals -------------------------

  private targetSegment(sessionId: string, target: SegmentRef): TargetRow {
    const row = this.db
      .prepare(
        'SELECT source_seq, kind, text, start_ms, applied_revision FROM events WHERE session_id = ? AND source_id = ? AND event_id = ?',
      )
      .get(sessionId, target.sourceId, target.eventId) as TargetRow | undefined;
    if (!row) {
      throw new ValidationError(
        `segment "${target.eventId}" from source "${target.sourceId}" does not exist`,
      );
    }
    if (row.kind !== 'final') {
      throw new ValidationError('only final segments can be corrected');
    }
    return row;
  }

  /**
   * The segment's current version: the revision that applied the original
   * event, or the latest correction's revision when corrections exist.
   */
  private targetVersion(sessionId: string, target: SegmentRef, appliedRevision: number): number {
    const row = this.db
      .prepare(
        'SELECT MAX(revision) AS m FROM corrections WHERE session_id = ? AND target_source_id = ? AND target_event_id = ?',
      )
      .get(sessionId, target.sourceId, target.eventId) as { m: number | null };
    return Math.max(appliedRevision, row.m ?? 0);
  }

  private latestCorrections(sessionId: string): Map<string, CorrectionInfo> {
    const rows = this.db
      .prepare(
        `SELECT correction_id, target_source_id, target_event_id, text, actor, reason, supersedes, revision
         FROM corrections WHERE session_id = ? ORDER BY revision`,
      )
      .all(sessionId) as CorrectionRow[];
    const map = new Map<string, CorrectionInfo>();
    for (const r of rows) {
      map.set(correctionKey(r.target_source_id, r.target_event_id), {
        correctionId: r.correction_id,
        text: r.text,
        actor: r.actor,
        reason: r.reason,
        revision: r.revision,
        supersedes: r.supersedes,
      });
    }
    return map;
  }

  private acquireOne(sessionId: string, target: SegmentRef, options: AcquireLeaseOptions): LeaseInfo {
    validateId('sessionId', sessionId);
    validateId('target.sourceId', target?.sourceId);
    validateId('target.eventId', target?.eventId);
    validateId('options.actor', options?.actor);
    if (!Number.isInteger(options.baseRevision) || options.baseRevision < 0) {
      throw new ValidationError('baseRevision must be a non-negative integer');
    }
    if (!Number.isFinite(options.ttlMs) || options.ttlMs <= 0) {
      throw new ValidationError('ttlMs must be a positive finite number');
    }

    const segment = this.targetSegment(sessionId, target);
    const version = this.targetVersion(sessionId, target, segment.applied_revision ?? 0);
    if (options.baseRevision < version) {
      throw new ReviewConflictError(
        'stale_base',
        `baseRevision ${options.baseRevision} is older than the segment's current version ${version}`,
      );
    }

    const now = this.nowFn();
    const existing = this.db
      .prepare(
        'SELECT lease_id, actor, expires_at FROM leases WHERE session_id = ? AND target_source_id = ? AND target_event_id = ?',
      )
      .get(sessionId, target.sourceId, target.eventId) as LeaseRow | undefined;
    if (existing && existing.expires_at > now) {
      throw new ReviewConflictError(
        'lease_held',
        `segment is already leased by "${existing.actor}" until ${existing.expires_at}`,
      );
    }

    const lease: LeaseInfo = { leaseId: randomUUID(), expiresAt: now + options.ttlMs };
    this.db
      .prepare(
        `INSERT INTO leases (session_id, target_source_id, target_event_id, lease_id, actor, base_revision, expires_at, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (session_id, target_source_id, target_event_id)
         DO UPDATE SET lease_id = excluded.lease_id, actor = excluded.actor,
                       base_revision = excluded.base_revision, expires_at = excluded.expires_at,
                       created_at = excluded.created_at`,
      )
      .run(sessionId, target.sourceId, target.eventId, lease.leaseId, options.actor, options.baseRevision, lease.expiresAt, now);
    return lease;
  }

  private submitOne(
    sessionId: string,
    target: SegmentRef,
    options: SubmitCorrectionOptions,
  ): SubmitCorrectionResult {
    validateId('sessionId', sessionId);
    validateId('target.sourceId', target?.sourceId);
    validateId('target.eventId', target?.eventId);
    validateId('options.leaseId', options?.leaseId);
    validateId('options.actor', options?.actor);
    if (typeof options?.text !== 'string') {
      throw new ValidationError('correction text must be a string');
    }
    if (typeof options.reason !== 'string') {
      throw new ValidationError('correction reason must be a string');
    }
    if (!Number.isInteger(options.baseRevision) || options.baseRevision < 0) {
      throw new ValidationError('baseRevision must be a non-negative integer');
    }

    const segment = this.targetSegment(sessionId, target);
    const now = this.nowFn();
    const lease = this.db
      .prepare(
        'SELECT lease_id, actor, base_revision, expires_at FROM leases WHERE session_id = ? AND target_source_id = ? AND target_event_id = ?',
      )
      .get(sessionId, target.sourceId, target.eventId) as LeaseRow | undefined;
    if (!lease || lease.lease_id !== options.leaseId) {
      throw new ReviewConflictError('no_lease', 'no live lease for this segment and leaseId');
    }
    if (lease.expires_at <= now) {
      throw new ReviewConflictError('lease_expired', `lease expired at ${lease.expires_at}`);
    }
    const version = this.targetVersion(sessionId, target, segment.applied_revision ?? 0);
    if (options.baseRevision !== lease.base_revision || options.baseRevision < version) {
      throw new ReviewConflictError(
        'stale_base',
        `baseRevision ${options.baseRevision} does not cover the segment's current version ${version}`,
      );
    }

    // Consume the lease and commit the correction in the same transaction as
    // the revision-stream append: a crash can never split them.
    this.db
      .prepare(
        'DELETE FROM leases WHERE session_id = ? AND target_source_id = ? AND target_event_id = ?',
      )
      .run(sessionId, target.sourceId, target.eventId);

    const previous = this.latestCorrections(sessionId).get(
      correctionKey(target.sourceId, target.eventId),
    );
    const session = this.db
      .prepare('SELECT last_revision FROM sessions WHERE session_id = ?')
      .get(sessionId) as SessionRow;
    const revision = session.last_revision + 1;
    const correctionId = randomUUID();
    this.db
      .prepare(
        `INSERT INTO corrections (session_id, correction_id, target_source_id, target_event_id, text, actor, reason, supersedes, revision, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        sessionId,
        correctionId,
        target.sourceId,
        target.eventId,
        options.text,
        options.actor,
        options.reason,
        previous?.correctionId ?? null,
        revision,
        now,
      );
    this.db
      .prepare('UPDATE sessions SET last_revision = ? WHERE session_id = ?')
      .run(revision, sessionId);
    const change: ChangeRecord = {
      type: 'correction',
      sourceId: target.sourceId,
      eventId: target.eventId,
      sourceSeq: segment.source_seq,
      text: options.text,
      startMs: segment.start_ms,
      correctionId,
      actor: options.actor,
      reason: options.reason,
      supersedes: previous?.correctionId ?? null,
    };
    this.db
      .prepare('INSERT INTO revisions (session_id, revision, change_json, created_at) VALUES (?, ?, ?, ?)')
      .run(sessionId, revision, JSON.stringify(change), Date.now());

    return { status: 'applied', revision, correctionId };
  }
}
