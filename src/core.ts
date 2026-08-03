import { randomUUID } from "node:crypto";
import type { DB } from "./db";
import { openDb } from "./db";
import { contentHash } from "./hash";
import {
  ConflictError,
  ValidationError,
  LeaseConflictError,
  LeaseExpiredError,
  NoLeaseError,
  StaleBaseError,
  ArchiveIntegrityError,
  ArchiveConflictError,
} from "./errors";
import {
  ARCHIVE_FORMAT,
  SUPPORTED_FORMATS,
  Chain,
  Hasher,
  canonicalStringify,
  splitExt,
} from "./archive";
import type {
  AcquireLeaseRequest,
  ApplyResult,
  ApplyOutcome,
  CorrectionResult,
  ExportOptions,
  ImportOptions,
  ImportResult,
  Lease,
  PullOptions,
  RevisionHubOptions,
  RevisionRecord,
  SegmentState,
  SessionSummary,
  Snapshot,
  SourceEvent,
  SubmitCorrectionRequest,
} from "./types";

interface SegmentRow {
  source_id: string;
  segment_id: string;
  kind: string;
  text: string;
  start_ms: number | null;
  end_ms: number | null;
  source_seq: number;
  event_id: string;
  finalized: number;
  revision: number;
  origin: string;
  actor: string | null;
}

interface EventRow {
  content_hash: string;
}

interface LeaseRow {
  session_id: string;
  source_id: string;
  segment_id: string;
  lease_id: string;
  actor: string;
  base_revision: number;
  acquired_at: number;
  expires_at: number;
  released: number;
}

/** Numeric rank so `final` always outranks `partial` regardless of seq. */
function kindRank(kind: string): number {
  return kind === "final" ? 1 : 0;
}

/**
 * Total order over the events of a single segment. Returns > 0 when `a`
 * should win over `b`. Because this is a total order, folding it over the set
 * of received events yields the same winner no matter the arrival order.
 */
function precedenceCmp(
  a: { kind: string; sourceSeq: number; eventId: string },
  b: { kind: string; sourceSeq: number; eventId: string },
): number {
  const rk = kindRank(a.kind) - kindRank(b.kind);
  if (rk !== 0) return rk;
  if (a.sourceSeq !== b.sourceSeq) return a.sourceSeq - b.sourceSeq;
  // Deterministic, extremely rare tie-break (same kind + seq, different id).
  return a.eventId < b.eventId ? -1 : a.eventId > b.eventId ? 1 : 0;
}

export class RevisionHub {
  private readonly db: DB;
  private readonly now: () => number;
  private closed = false;

  constructor(opts: RevisionHubOptions) {
    this.db = openDb(opts);
    this.now = opts.clock ?? Date.now;
  }

  /** Open a hub backed by a SQLite file (or ":memory:"). */
  static open(opts: RevisionHubOptions): RevisionHub {
    return new RevisionHub(opts);
  }

  /** Underlying database handle (escape hatch for tests/tools). */
  get database(): DB {
    return this.db;
  }

  close(): void {
    if (!this.closed) {
      this.db.close();
      this.closed = true;
    }
  }

  private validate(ev: SourceEvent): void {
    const req: Array<keyof SourceEvent> = ["sessionId", "sourceId", "segmentId", "eventId"];
    for (const k of req) {
      if (typeof ev[k] !== "string" || (ev[k] as string).length === 0) {
        throw new ValidationError(`missing or empty field: ${String(k)}`, { field: k });
      }
    }
    if (ev.kind !== "partial" && ev.kind !== "final") {
      throw new ValidationError(`invalid kind: ${String(ev.kind)}`, { kind: ev.kind });
    }
    if (typeof ev.text !== "string") {
      throw new ValidationError("text must be a string");
    }
    if (!Number.isInteger(ev.sourceSeq) || ev.sourceSeq < 0) {
      throw new ValidationError("sourceSeq must be a non-negative integer", {
        sourceSeq: ev.sourceSeq,
      });
    }
    for (const t of ["startMs", "endMs"] as const) {
      const v = ev[t];
      if (v !== undefined && (typeof v !== "number" || !Number.isFinite(v))) {
        throw new ValidationError(`${t} must be a finite number when provided`);
      }
    }
  }

  /**
   * Apply one source event. Idempotent for exact re-deliveries; rejects
   * same-id/different-content; ignores stale partials and post-final partials.
   * When (and only when) the segment's resolved state changes, a new
   * session-monotonic revision is assigned atomically.
   */
  apply(ev: SourceEvent): ApplyResult {
    this.validate(ev);
    const hash = contentHash(ev);
    const startMs = ev.startMs ?? null;
    const endMs = ev.endMs ?? null;

    const runner = this.db.transaction((): ApplyResult => {
      // 1) Idempotency / conflict on the source-stable event id.
      const existing = this.db
        .prepare(
          "SELECT content_hash FROM events WHERE session_id=? AND source_id=? AND event_id=?",
        )
        .get(ev.sessionId, ev.sourceId, ev.eventId) as EventRow | undefined;
      if (existing) {
        if (existing.content_hash === hash) {
          return this.result(ev, "duplicate", null);
        }
        throw new ConflictError("eventId re-delivered with different content", {
          sessionId: ev.sessionId,
          sourceId: ev.sourceId,
          eventId: ev.eventId,
        });
      }

      // 2) Persist the raw event (append-only ground truth).
      const now = this.now();
      this.db
        .prepare(
          `INSERT INTO events
             (session_id, source_id, event_id, segment_id, source_seq, kind, text,
              start_ms, end_ms, content_hash, received_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
        )
        .run(
          ev.sessionId,
          ev.sourceId,
          ev.eventId,
          ev.segmentId,
          ev.sourceSeq,
          ev.kind,
          ev.text,
          startMs,
          endMs,
          hash,
          now,
        );

      // 3) Resolve winner incrementally against the current segment state.
      const seg = this.db
        .prepare(
          `SELECT source_id, segment_id, kind, text, start_ms, end_ms, source_seq,
                  event_id, finalized, revision, origin, actor
             FROM segments
            WHERE session_id=? AND source_id=? AND segment_id=?`,
        )
        .get(ev.sessionId, ev.sourceId, ev.segmentId) as SegmentRow | undefined;

      let outcome: ApplyOutcome;
      if (!seg) {
        outcome = "created";
      } else if (seg.origin === "correction") {
        // A human correction is authoritative: raw recognition events never
        // roll it back (extends the final-no-rollback rule to reviews).
        outcome = "superseded";
      } else {
        const wins =
          precedenceCmp(
            { kind: ev.kind, sourceSeq: ev.sourceSeq, eventId: ev.eventId },
            { kind: seg.kind, sourceSeq: seg.source_seq, eventId: seg.event_id },
          ) > 0;
        outcome = wins ? "updated" : "superseded";
      }

      if (outcome === "superseded") {
        return this.result(ev, outcome, null);
      }

      // 4) Effective change → allocate the next session revision atomically.
      const revision = this.allocateRevision(ev.sessionId);

      // 5) Upsert resolved segment state (recognition provenance).
      this.upsertSegment({
        sessionId: ev.sessionId,
        sourceId: ev.sourceId,
        segmentId: ev.segmentId,
        kind: ev.kind,
        text: ev.text,
        startMs,
        endMs,
        sourceSeq: ev.sourceSeq,
        eventId: ev.eventId,
        finalized: ev.kind === "final" ? 1 : 0,
        revision,
        origin: "recognition",
        actor: null,
      });

      // 6) Append to the durable revision stream (recognition revision).
      this.insertRevision({
        sessionId: ev.sessionId,
        revision,
        sourceId: ev.sourceId,
        segmentId: ev.segmentId,
        kind: ev.kind,
        text: ev.text,
        startMs,
        endMs,
        eventId: ev.eventId,
        sourceSeq: ev.sourceSeq,
        createdAt: now,
        origin: "recognition",
        actor: null,
        reason: null,
        correctionId: null,
        supersedesRevision: null,
      });

      return this.result(ev, outcome, revision);
    });

    // IMMEDIATE takes the write lock up front, serialising writers across
    // processes so revision allocation is race-free.
    return runner.immediate();
  }

  /** Allocate the next monotonic revision for a session (call inside a tx). */
  private allocateRevision(sessionId: string): number {
    this.db
      .prepare(
        `INSERT INTO sessions (session_id, head_revision) VALUES (?, 1)
           ON CONFLICT(session_id) DO UPDATE SET head_revision = head_revision + 1`,
      )
      .run(sessionId);
    return (
      this.db
        .prepare("SELECT head_revision AS r FROM sessions WHERE session_id=?")
        .get(sessionId) as { r: number }
    ).r;
  }

  /** Upsert the resolved winner for a segment (call inside a tx). */
  private upsertSegment(s: {
    sessionId: string;
    sourceId: string;
    segmentId: string;
    kind: string;
    text: string;
    startMs: number | null;
    endMs: number | null;
    sourceSeq: number;
    eventId: string;
    finalized: number;
    revision: number;
    origin: string;
    actor: string | null;
  }): void {
    this.db
      .prepare(
        `INSERT INTO segments
           (session_id, source_id, segment_id, kind, text, start_ms, end_ms,
            source_seq, event_id, finalized, revision, origin, actor)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
         ON CONFLICT(session_id, source_id, segment_id) DO UPDATE SET
           kind=excluded.kind, text=excluded.text, start_ms=excluded.start_ms,
           end_ms=excluded.end_ms, source_seq=excluded.source_seq,
           event_id=excluded.event_id, finalized=excluded.finalized,
           revision=excluded.revision, origin=excluded.origin, actor=excluded.actor`,
      )
      .run(
        s.sessionId,
        s.sourceId,
        s.segmentId,
        s.kind,
        s.text,
        s.startMs,
        s.endMs,
        s.sourceSeq,
        s.eventId,
        s.finalized,
        s.revision,
        s.origin,
        s.actor,
      );
  }

  /** Append one entry to the durable revision stream (call inside a tx). */
  private insertRevision(r: RevisionRecord): void {
    this.db
      .prepare(
        `INSERT INTO revisions
           (session_id, revision, source_id, segment_id, kind, text, start_ms,
            end_ms, event_id, source_seq, created_at, origin, actor, reason,
            correction_id, supersedes_revision)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      )
      .run(
        r.sessionId,
        r.revision,
        r.sourceId,
        r.segmentId,
        r.kind,
        r.text,
        r.startMs,
        r.endMs,
        r.eventId,
        r.sourceSeq,
        r.createdAt,
        r.origin,
        r.actor,
        r.reason,
        r.correctionId,
        r.supersedesRevision,
      );
  }

  /** Apply many events; returns per-event results in input order. */
  applyBatch(events: SourceEvent[]): ApplyResult[] {
    return events.map((e) => this.apply(e));
  }

  private result(
    ev: SourceEvent,
    outcome: ApplyOutcome,
    revision: number | null,
  ): ApplyResult {
    return {
      sessionId: ev.sessionId,
      sourceId: ev.sourceId,
      segmentId: ev.segmentId,
      eventId: ev.eventId,
      outcome,
      effective: revision !== null,
      revision,
    };
  }

  /** Current head revision for a session (0 if none). */
  headRevision(sessionId: string): number {
    const row = this.db
      .prepare("SELECT head_revision AS r FROM sessions WHERE session_id=?")
      .get(sessionId) as { r: number } | undefined;
    return row?.r ?? 0;
  }

  /** Build the deterministic, queryable snapshot for a session. */
  getSnapshot(sessionId: string): Snapshot {
    const rows = this.db
      .prepare(
        `SELECT source_id, segment_id, kind, text, start_ms, end_ms, source_seq,
                event_id, revision, origin, actor
           FROM segments
          WHERE session_id=?
          ORDER BY (start_ms IS NULL), start_ms, source_id, segment_id`,
      )
      .all(sessionId) as SegmentRow[];

    const segments: SegmentState[] = rows.map((r) => ({
      sourceId: r.source_id,
      segmentId: r.segment_id,
      kind: r.kind as SegmentState["kind"],
      text: r.text,
      startMs: r.start_ms,
      endMs: r.end_ms,
      sourceSeq: r.source_seq,
      eventId: r.event_id,
      revision: r.revision,
      origin: r.origin as SegmentState["origin"],
      actor: r.actor,
    }));

    return {
      sessionId,
      headRevision: this.headRevision(sessionId),
      segments,
      summary: this.summarize(segments),
    };
  }

  private summarize(segments: SegmentState[]): SessionSummary {
    const perSource: Record<string, { segments: number; finals: number; partials: number }> = {};
    let finalCount = 0;
    let partialCount = 0;
    const texts: string[] = [];
    for (const s of segments) {
      const bucket = (perSource[s.sourceId] ??= { segments: 0, finals: 0, partials: 0 });
      bucket.segments += 1;
      if (s.kind === "final") {
        finalCount += 1;
        bucket.finals += 1;
      } else {
        partialCount += 1;
        bucket.partials += 1;
      }
      if (s.text.length > 0) texts.push(s.text);
    }
    // Rebuild perSource with sorted keys for stable serialization.
    const sortedPerSource: SessionSummary["perSource"] = {};
    for (const key of Object.keys(perSource).sort()) {
      sortedPerSource[key] = perSource[key]!;
    }
    return {
      segmentCount: segments.length,
      finalCount,
      partialCount,
      fullText: texts.join(" "),
      perSource: sortedPerSource,
    };
  }

  /**
   * Read revisions for a consumer starting strictly after its durable cursor
   * (or after {@link PullOptions.afterRevision} when provided). Does not move
   * the cursor; call {@link ack} to advance it.
   */
  pull(sessionId: string, consumerId: string, opts: PullOptions = {}): RevisionRecord[] {
    if (!consumerId) throw new ValidationError("consumerId is required");
    const limit = opts.limit ?? 100;
    if (!Number.isInteger(limit) || limit <= 0) {
      throw new ValidationError("limit must be a positive integer");
    }
    const after = opts.afterRevision ?? this.getCursor(sessionId, consumerId, true);
    const rows = this.db
      .prepare(
        `SELECT session_id, revision, source_id, segment_id, kind, text, start_ms,
                end_ms, event_id, source_seq, created_at, origin, actor, reason,
                correction_id, supersedes_revision
           FROM revisions
          WHERE session_id=? AND revision > ?
          ORDER BY revision ASC
          LIMIT ?`,
      )
      .all(sessionId, after, limit) as Array<{
      session_id: string;
      revision: number;
      source_id: string;
      segment_id: string;
      kind: string;
      text: string;
      start_ms: number | null;
      end_ms: number | null;
      event_id: string;
      source_seq: number;
      created_at: number;
      origin: string;
      actor: string | null;
      reason: string | null;
      correction_id: string | null;
      supersedes_revision: number | null;
    }>;
    return rows.map((r) => ({
      sessionId: r.session_id,
      revision: r.revision,
      sourceId: r.source_id,
      segmentId: r.segment_id,
      kind: r.kind as RevisionRecord["kind"],
      text: r.text,
      startMs: r.start_ms,
      endMs: r.end_ms,
      eventId: r.event_id,
      sourceSeq: r.source_seq,
      createdAt: r.created_at,
      origin: r.origin as RevisionRecord["origin"],
      actor: r.actor,
      reason: r.reason,
      correctionId: r.correction_id,
      supersedesRevision: r.supersedes_revision,
    }));
  }

  /**
   * Durably advance a consumer's cursor. Monotonic: acking a revision lower
   * than the stored cursor is a no-op, so already-acked ranges are never
   * re-sent and unacked ranges are never skipped.
   */
  ack(sessionId: string, consumerId: string, uptoRevision: number): number {
    if (!consumerId) throw new ValidationError("consumerId is required");
    if (!Number.isInteger(uptoRevision) || uptoRevision < 0) {
      throw new ValidationError("uptoRevision must be a non-negative integer");
    }
    const runner = this.db.transaction((): number => {
      const now = this.now();
      this.db
        .prepare(
          `INSERT INTO consumers (session_id, consumer_id, cursor, updated_at)
             VALUES (?,?,?,?)
           ON CONFLICT(session_id, consumer_id) DO UPDATE SET
             cursor = MAX(cursor, excluded.cursor),
             updated_at = excluded.updated_at`,
        )
        .run(sessionId, consumerId, uptoRevision, now);
      return (
        this.db
          .prepare(
            "SELECT cursor AS c FROM consumers WHERE session_id=? AND consumer_id=?",
          )
          .get(sessionId, consumerId) as { c: number }
      ).c;
    });
    return runner.immediate();
  }

  /**
   * Get a consumer's stored cursor. When `autoCreate` is true a missing
   * consumer is registered at cursor 0.
   */
  getCursor(sessionId: string, consumerId: string, autoCreate = false): number {
    const row = this.db
      .prepare("SELECT cursor AS c FROM consumers WHERE session_id=? AND consumer_id=?")
      .get(sessionId, consumerId) as { c: number } | undefined;
    if (row) return row.c;
    if (autoCreate) {
      this.db
        .prepare(
          `INSERT INTO consumers (session_id, consumer_id, cursor, updated_at)
             VALUES (?,?,0,?)
           ON CONFLICT(session_id, consumer_id) DO NOTHING`,
        )
        .run(sessionId, consumerId, this.now());
    }
    return 0;
  }

  // ---------------------------------------------------------------------------
  // Human review: correction leases
  // ---------------------------------------------------------------------------

  /**
   * Acquire a time-boxed correction lease on a segment. Competing acquisitions
   * are serialised by the IMMEDIATE write lock, so exactly one wins while a live
   * lease is held; others get a {@link LeaseConflictError}. An expired lease is
   * transparently taken over. The `baseRevision` records the state the reviewer
   * is correcting against and is enforced at submit time.
   */
  acquireLease(req: AcquireLeaseRequest): Lease {
    if (!req.actor) throw new ValidationError("actor is required");
    if (!req.sessionId || !req.sourceId || !req.segmentId) {
      throw new ValidationError("sessionId, sourceId, segmentId are required");
    }
    if (!Number.isInteger(req.baseRevision) || req.baseRevision < 0) {
      throw new ValidationError("baseRevision must be a non-negative integer");
    }
    if (!Number.isInteger(req.ttlMs) || req.ttlMs <= 0) {
      throw new ValidationError("ttlMs must be a positive integer");
    }

    const runner = this.db.transaction((): Lease => {
      const now = this.now();
      const existing = this.db
        .prepare(
          `SELECT * FROM leases WHERE session_id=? AND source_id=? AND segment_id=?`,
        )
        .get(req.sessionId, req.sourceId, req.segmentId) as LeaseRow | undefined;

      // A live, unreleased lease held by anyone blocks a new claim.
      if (existing && existing.released === 0 && existing.expires_at > now) {
        throw new LeaseConflictError("segment is already leased", {
          sessionId: req.sessionId,
          sourceId: req.sourceId,
          segmentId: req.segmentId,
          heldBy: existing.actor,
          expiresAt: existing.expires_at,
        });
      }

      const leaseId = randomUUID();
      const expiresAt = now + req.ttlMs;
      // Upsert: takes over an expired/released lease for the same segment.
      this.db
        .prepare(
          `INSERT INTO leases
             (session_id, source_id, segment_id, lease_id, actor, base_revision,
              acquired_at, expires_at, released)
           VALUES (?,?,?,?,?,?,?,?,0)
           ON CONFLICT(session_id, source_id, segment_id) DO UPDATE SET
             lease_id=excluded.lease_id, actor=excluded.actor,
             base_revision=excluded.base_revision, acquired_at=excluded.acquired_at,
             expires_at=excluded.expires_at, released=0`,
        )
        .run(
          req.sessionId,
          req.sourceId,
          req.segmentId,
          leaseId,
          req.actor,
          req.baseRevision,
          now,
          expiresAt,
        );

      return {
        sessionId: req.sessionId,
        sourceId: req.sourceId,
        segmentId: req.segmentId,
        leaseId,
        actor: req.actor,
        baseRevision: req.baseRevision,
        acquiredAt: now,
        expiresAt,
      };
    });
    return runner.immediate();
  }

  /**
   * Submit a correction under a held lease. In one atomic transaction this:
   *  - rejects an unknown/mismatched lease         → {@link NoLeaseError}
   *  - rejects a lease whose TTL elapsed            → {@link LeaseExpiredError}
   *  - rejects a base that no longer matches state  → {@link StaleBaseError}
   *
   * On success it does NOT touch the raw `events` (recognition is preserved).
   * Instead it records lineage in `corrections`, flips the segment to
   * origin='correction', and appends a new revision to the SAME durable stream
   * that existing consumers read. Idempotent on `correctionId`.
   */
  submitCorrection(req: SubmitCorrectionRequest): CorrectionResult {
    if (!req.leaseId) throw new ValidationError("leaseId is required");
    if (!req.actor) throw new ValidationError("actor is required");
    if (typeof req.text !== "string") throw new ValidationError("text must be a string");
    if (typeof req.reason !== "string" || req.reason.length === 0) {
      throw new ValidationError("reason is required");
    }

    const runner = this.db.transaction((): CorrectionResult => {
      const now = this.now();
      const lease = this.db
        .prepare(`SELECT * FROM leases WHERE lease_id=?`)
        .get(req.leaseId) as LeaseRow | undefined;

      if (!lease || lease.actor !== req.actor) {
        throw new NoLeaseError("no active lease matches (leaseId, actor)", {
          leaseId: req.leaseId,
          actor: req.actor,
        });
      }

      // Idempotent replay: a prior correction with this id already landed.
      if (req.correctionId) {
        const prior = this.db
          .prepare(
            `SELECT * FROM corrections WHERE session_id=? AND correction_id=?`,
          )
          .get(lease.session_id, req.correctionId) as
          | {
              source_id: string;
              segment_id: string;
              actor: string;
              revision: number;
              supersedes_revision: number;
            }
          | undefined;
        if (prior) {
          return {
            sessionId: lease.session_id,
            sourceId: prior.source_id,
            segmentId: prior.segment_id,
            correctionId: req.correctionId,
            actor: prior.actor,
            revision: prior.revision,
            supersedesRevision: prior.supersedes_revision,
            applied: false,
          };
        }
      }

      if (lease.released === 1) {
        throw new NoLeaseError("lease already consumed or released", {
          leaseId: req.leaseId,
        });
      }
      if (lease.expires_at <= now) {
        throw new LeaseExpiredError("lease TTL elapsed before submit", {
          leaseId: req.leaseId,
          expiresAt: lease.expires_at,
          now,
        });
      }

      // The segment must still be at the revision the reviewer based on.
      const seg = this.db
        .prepare(
          `SELECT revision FROM segments
            WHERE session_id=? AND source_id=? AND segment_id=?`,
        )
        .get(lease.session_id, lease.source_id, lease.segment_id) as
        | { revision: number }
        | undefined;
      const currentRevision = seg?.revision ?? 0;
      if (currentRevision !== lease.base_revision) {
        throw new StaleBaseError("segment changed since lease was taken", {
          leaseId: req.leaseId,
          baseRevision: lease.base_revision,
          currentRevision,
        });
      }

      // Read the segment's current shape so the correction preserves timing and
      // presents as a settled ("final") authoritative state.
      const full = this.db
        .prepare(
          `SELECT kind, text, start_ms, end_ms, source_seq, event_id
             FROM segments WHERE session_id=? AND source_id=? AND segment_id=?`,
        )
        .get(lease.session_id, lease.source_id, lease.segment_id) as
        | {
            kind: string;
            text: string;
            start_ms: number | null;
            end_ms: number | null;
            source_seq: number;
            event_id: string;
          }
        | undefined;
      if (!full) {
        // Correcting a segment that was never recognised is a stale base.
        throw new StaleBaseError("segment does not exist", {
          leaseId: req.leaseId,
        });
      }

      const correctionId = req.correctionId ?? randomUUID();
      const revision = this.allocateRevision(lease.session_id);

      // Segment now carries correction provenance; recognition can't roll back.
      this.upsertSegment({
        sessionId: lease.session_id,
        sourceId: lease.source_id,
        segmentId: lease.segment_id,
        kind: "final",
        text: req.text,
        startMs: full.start_ms,
        endMs: full.end_ms,
        sourceSeq: full.source_seq,
        eventId: full.event_id,
        finalized: 1,
        revision,
        origin: "correction",
        actor: req.actor,
      });

      // Lineage row (separate from raw events).
      this.db
        .prepare(
          `INSERT INTO corrections
             (session_id, correction_id, source_id, segment_id, actor, reason,
              text, base_revision, revision, supersedes_revision, lease_id, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
        )
        .run(
          lease.session_id,
          correctionId,
          lease.source_id,
          lease.segment_id,
          req.actor,
          req.reason,
          req.text,
          lease.base_revision,
          revision,
          lease.base_revision,
          req.leaseId,
          now,
        );

      // Same durable stream that existing consumers read.
      this.insertRevision({
        sessionId: lease.session_id,
        revision,
        sourceId: lease.source_id,
        segmentId: lease.segment_id,
        kind: "final",
        text: req.text,
        startMs: full.start_ms,
        endMs: full.end_ms,
        eventId: full.event_id,
        sourceSeq: full.source_seq,
        createdAt: now,
        origin: "correction",
        actor: req.actor,
        reason: req.reason,
        correctionId,
        supersedesRevision: lease.base_revision,
      });

      // Consume the lease so it can't be reused.
      this.db
        .prepare(
          `UPDATE leases SET released=1 WHERE session_id=? AND source_id=? AND segment_id=?`,
        )
        .run(lease.session_id, lease.source_id, lease.segment_id);

      return {
        sessionId: lease.session_id,
        sourceId: lease.source_id,
        segmentId: lease.segment_id,
        correctionId,
        actor: req.actor,
        revision,
        supersedesRevision: lease.base_revision,
        applied: true,
      };
    });
    return runner.immediate();
  }

  /** Fetch the current lease for a segment, if any (including expired ones). */
  getLease(sessionId: string, sourceId: string, segmentId: string): Lease | null {
    const row = this.db
      .prepare(
        `SELECT * FROM leases WHERE session_id=? AND source_id=? AND segment_id=?`,
      )
      .get(sessionId, sourceId, segmentId) as LeaseRow | undefined;
    if (!row || row.released === 1) return null;
    return {
      sessionId: row.session_id,
      sourceId: row.source_id,
      segmentId: row.segment_id,
      leaseId: row.lease_id,
      actor: row.actor,
      baseRevision: row.base_revision,
      acquiredAt: row.acquired_at,
      expiresAt: row.expires_at,
    };
  }

  /**
   * Explicitly release a lease held by `actor` without submitting (e.g. the
   * reviewer cancelled). No-op if the lease is missing or held by someone else.
   */
  releaseLease(leaseId: string, actor: string): boolean {
    const runner = this.db.transaction((): boolean => {
      const res = this.db
        .prepare(
          `UPDATE leases SET released=1 WHERE lease_id=? AND actor=? AND released=0`,
        )
        .run(leaseId, actor);
      return res.changes > 0;
    });
    return runner.immediate();
  }

  // ---------------------------------------------------------------------------
  // Compliance handoff: self-contained session archives
  // ---------------------------------------------------------------------------

  /**
   * Stream a self-contained, versioned, tamper-evident archive of one session
   * as newline-delimited JSON records (yielded WITHOUT trailing newlines; the
   * caller joins with "\n"). The generator pulls rows lazily via SQLite cursors
   * so memory stays bounded for large sessions.
   *
   * Order: header → session → events → segments → revisions → corrections →
   * leases → consumers → trailer. The full correction lineage (actor,
   * baseRevision, reason, supersedes) rides along in the `correction` and
   * `revision` records. A trailer pins the record count and both digests so any
   * reorder/mutation/truncation is detected on import.
   */
  *exportSession(sessionId: string, opts: ExportOptions = {}): IterableIterator<string> {
    const secret = opts.secret;
    const chain = new Chain(secret);
    const content = new Hasher(secret);
    const extMap = this.loadExt(sessionId);

    const emit = (obj: Record<string, unknown>, extKey?: string): string => {
      const type = obj.type as string;
      const ext = extKey !== undefined ? extMap.get(`${type}\u0000${extKey}`) : undefined;
      const merged = ext ? { ...obj, ...ext } : obj;
      return canonicalStringify(merged);
    };

    // 1) Header — folded into the chain but NOT the content digest, so the
    //    volatile createdAt never affects the idempotency key.
    const headRevision = this.headRevision(sessionId);
    const headerLine = emit({
      type: "header",
      format: ARCHIVE_FORMAT,
      sessionId,
      createdAt: this.now(),
      keyed: secret !== undefined,
    }, "");
    chain.update(headerLine);
    yield headerLine;

    let count = 0;
    const body = (obj: Record<string, unknown>, extKey?: string): string => {
      const line = emit(obj, extKey);
      chain.update(line);
      content.update(line);
      count += 1;
      return line;
    };

    // 2) session record.
    yield body({ type: "session", sessionId, headRevision }, "");

    // 3) events (raw recognition, preserved verbatim).
    for (const r of this.iterate(
      `SELECT source_id, event_id, segment_id, source_seq, kind, text, start_ms,
              end_ms, content_hash, received_at
         FROM events WHERE session_id=? ORDER BY source_id, event_id`,
      sessionId,
    )) {
      yield body(
        {
          type: "event",
          sessionId,
          sourceId: r.source_id,
          eventId: r.event_id,
          segmentId: r.segment_id,
          sourceSeq: r.source_seq,
          kind: r.kind,
          text: r.text,
          startMs: r.start_ms,
          endMs: r.end_ms,
          contentHash: r.content_hash,
          receivedAt: r.received_at,
        },
        `${r.source_id}\u0000${r.event_id}`,
      );
    }

    // 4) segments (resolved winners with provenance).
    for (const r of this.iterate(
      `SELECT source_id, segment_id, kind, text, start_ms, end_ms, source_seq,
              event_id, finalized, revision, origin, actor
         FROM segments WHERE session_id=? ORDER BY source_id, segment_id`,
      sessionId,
    )) {
      yield body(
        {
          type: "segment",
          sessionId,
          sourceId: r.source_id,
          segmentId: r.segment_id,
          kind: r.kind,
          text: r.text,
          startMs: r.start_ms,
          endMs: r.end_ms,
          sourceSeq: r.source_seq,
          eventId: r.event_id,
          finalized: r.finalized,
          revision: r.revision,
          origin: r.origin,
          actor: r.actor,
        },
        `${r.source_id}\u0000${r.segment_id}`,
      );
    }

    // 5) revision stream (recognition + corrections, full lineage fields).
    for (const r of this.iterate(
      `SELECT revision, source_id, segment_id, kind, text, start_ms, end_ms,
              event_id, source_seq, created_at, origin, actor, reason,
              correction_id, supersedes_revision
         FROM revisions WHERE session_id=? ORDER BY revision`,
      sessionId,
    )) {
      yield body(
        {
          type: "revision",
          sessionId,
          revision: r.revision,
          sourceId: r.source_id,
          segmentId: r.segment_id,
          kind: r.kind,
          text: r.text,
          startMs: r.start_ms,
          endMs: r.end_ms,
          eventId: r.event_id,
          sourceSeq: r.source_seq,
          createdAt: r.created_at,
          origin: r.origin,
          actor: r.actor,
          reason: r.reason,
          correctionId: r.correction_id,
          supersedesRevision: r.supersedes_revision,
        },
        String(r.revision),
      );
    }

    // 6) correction lineage (actor, baseRevision, reason, supersedes, lease).
    for (const r of this.iterate(
      `SELECT correction_id, source_id, segment_id, actor, reason, text,
              base_revision, revision, supersedes_revision, lease_id, created_at
         FROM corrections WHERE session_id=? ORDER BY revision`,
      sessionId,
    )) {
      yield body(
        {
          type: "correction",
          sessionId,
          correctionId: r.correction_id,
          sourceId: r.source_id,
          segmentId: r.segment_id,
          actor: r.actor,
          reason: r.reason,
          text: r.text,
          baseRevision: r.base_revision,
          revision: r.revision,
          supersedesRevision: r.supersedes_revision,
          leaseId: r.lease_id,
          createdAt: r.created_at,
        },
        r.correction_id as string,
      );
    }

    // 7) leases (so an in-flight review survives the handoff).
    for (const r of this.iterate(
      `SELECT source_id, segment_id, lease_id, actor, base_revision, acquired_at,
              expires_at, released
         FROM leases WHERE session_id=? ORDER BY source_id, segment_id`,
      sessionId,
    )) {
      yield body(
        {
          type: "lease",
          sessionId,
          sourceId: r.source_id,
          segmentId: r.segment_id,
          leaseId: r.lease_id,
          actor: r.actor,
          baseRevision: r.base_revision,
          acquiredAt: r.acquired_at,
          expiresAt: r.expires_at,
          released: r.released,
        },
        `${r.source_id}\u0000${r.segment_id}`,
      );
    }

    // 8) consumer cursors (resumable read positions).
    for (const r of this.iterate(
      `SELECT consumer_id, cursor, updated_at
         FROM consumers WHERE session_id=? ORDER BY consumer_id`,
      sessionId,
    )) {
      yield body(
        {
          type: "consumer",
          sessionId,
          consumerId: r.consumer_id,
          cursor: r.cursor,
          updatedAt: r.updated_at,
        },
        r.consumer_id as string,
      );
    }

    // 9) trailer — the tamper-evidence anchor. Not folded into itself.
    yield canonicalStringify({
      type: "trailer",
      count,
      content: content.digest(),
      chain: chain.digest(),
    });
  }

  /** Materialise a full archive to a single string (convenience over the stream). */
  exportSessionToString(sessionId: string, opts: ExportOptions = {}): string {
    let out = "";
    for (const line of this.exportSession(sessionId, opts)) out += line + "\n";
    return out;
  }

  /**
   * Import a session archive produced by {@link exportSession}. Accepts the
   * archive as a single string or as an Iterable of arbitrary string chunks
   * (streamed reading; chunk boundaries need not align with lines).
   *
   * Guarantees:
   *  - The whole import runs in ONE IMMEDIATE transaction, and the trailer's
   *    digests/count/version are verified before commit. Any reorder, mutation,
   *    truncation, or unsupported version throws {@link ArchiveIntegrityError}
   *    and rolls back — no half-imported session ever becomes visible.
   *  - Re-importing the identical archive is idempotent (no-op). A *different*
   *    archive for a session that already has content throws
   *    {@link ArchiveConflictError}.
   *  - Into an empty DB, the resulting snapshot, revision history, and resumable
   *    cursors are equivalent to the export side.
   */
  importSession(source: string | Iterable<string>, opts: ImportOptions = {}): ImportResult {
    const secret = opts.secret;
    const lines = collectLines(source);

    const runner = this.db.transaction((): ImportResult => {
      const chain = new Chain(secret);
      const content = new Hasher(secret);

      if (lines.length === 0) {
        throw new ArchiveIntegrityError("empty archive");
      }

      // --- Header ---
      const headerLine = lines[0]!;
      const header = parseJson(headerLine, "header");
      if (header.type !== "header") {
        throw new ArchiveIntegrityError("archive does not start with a header", {
          got: header.type,
        });
      }
      const format = header.format;
      if (typeof format !== "number" || !SUPPORTED_FORMATS.has(format)) {
        throw new ArchiveIntegrityError("unsupported archive format version", {
          format,
          supported: [...SUPPORTED_FORMATS],
        });
      }
      const sessionId = header.sessionId;
      if (typeof sessionId !== "string" || sessionId.length === 0) {
        throw new ArchiveIntegrityError("header missing sessionId");
      }
      chain.update(headerLine);

      // --- Body ---
      // We buffer parsed body records so idempotency/conflict can be decided
      // (which needs the content digest, known only after the whole body) before
      // touching domain tables. Integrity is still verified line-by-line.
      interface Parsed {
        type: string;
        known: Record<string, unknown>;
        ext: Record<string, unknown>;
      }
      const bodyRecords: Parsed[] = [];
      let trailer: Record<string, unknown> | null = null;
      let count = 0;

      for (let i = 1; i < lines.length; i++) {
        const line = lines[i]!;
        const obj = parseJson(line, "record");
        if (obj.type === "trailer") {
          if (i !== lines.length - 1) {
            throw new ArchiveIntegrityError("records found after trailer", { index: i });
          }
          trailer = obj;
          break;
        }
        chain.update(line);
        content.update(line);
        count += 1;
        const { known, ext } = splitExt(obj.type as string, obj);
        bodyRecords.push({ type: obj.type as string, known, ext });
      }

      // --- Trailer verification (truncation / reorder / tamper) ---
      if (!trailer) {
        throw new ArchiveIntegrityError("archive is truncated: missing trailer");
      }
      if (trailer.count !== count) {
        throw new ArchiveIntegrityError("record count mismatch (truncated or tampered)", {
          expected: trailer.count,
          actual: count,
        });
      }
      const contentDigestValue = content.digest();
      if (trailer.content !== contentDigestValue) {
        throw new ArchiveIntegrityError("content digest mismatch (reordered or tampered)");
      }
      if (trailer.chain !== chain.digest()) {
        throw new ArchiveIntegrityError("chain digest mismatch (reordered or tampered)");
      }

      // --- Idempotency / conflict ---
      const ledger = this.db
        .prepare("SELECT digest, format FROM imported_archives WHERE session_id=?")
        .get(sessionId) as { digest: string; format: number } | undefined;
      if (ledger) {
        if (ledger.digest === contentDigestValue) {
          return {
            sessionId,
            digest: contentDigestValue,
            format,
            recordCount: count,
            headRevision: this.headRevision(sessionId),
            imported: false,
          };
        }
        throw new ArchiveConflictError("a different archive was already imported", {
          sessionId,
        });
      }
      // A session with pre-existing content but no ledger entry (e.g. a live
      // session) must not be silently overwritten.
      const existing = this.db
        .prepare("SELECT 1 FROM revisions WHERE session_id=? LIMIT 1")
        .get(sessionId);
      if (existing) {
        throw new ArchiveConflictError("session already has content; refusing to import", {
          sessionId,
        });
      }

      // --- Apply (only reached once integrity + idempotency are settled) ---
      this.applyImportedRecords(sessionId, bodyRecords);

      // Record ext (unknown forward-compat fields) for faithful re-export.
      for (const rec of bodyRecords) {
        if (Object.keys(rec.ext).length === 0) continue;
        const key = this.extKeyFor(rec.type, rec.known);
        this.db
          .prepare(
            `INSERT INTO archive_ext (session_id, record_type, record_key, ext_json)
               VALUES (?,?,?,?)
             ON CONFLICT(session_id, record_type, record_key) DO UPDATE SET
               ext_json = excluded.ext_json`,
          )
          .run(sessionId, rec.type, key, canonicalStringify(rec.ext));
      }
      // Preserve header-level unknown fields too.
      {
        const { ext } = splitExt("header", header);
        if (Object.keys(ext).length > 0) {
          this.db
            .prepare(
              `INSERT INTO archive_ext (session_id, record_type, record_key, ext_json)
                 VALUES (?,?, '', ?)
               ON CONFLICT(session_id, record_type, record_key) DO UPDATE SET
                 ext_json = excluded.ext_json`,
            )
            .run(sessionId, "header", canonicalStringify(ext));
        }
      }

      this.db
        .prepare(
          `INSERT INTO imported_archives (session_id, digest, format, imported_at)
             VALUES (?,?,?,?)`,
        )
        .run(sessionId, contentDigestValue, format, this.now());

      return {
        sessionId,
        digest: contentDigestValue,
        format,
        recordCount: count,
        headRevision: this.headRevision(sessionId),
        imported: true,
      };
    });

    return runner.immediate();
  }

  /** Insert the parsed body records into the domain tables (call inside a tx). */
  private applyImportedRecords(
    sessionId: string,
    records: Array<{ type: string; known: Record<string, unknown> }>,
  ): void {
    const num = (v: unknown): number | null =>
      v === null || v === undefined ? null : Number(v);
    const reqNum = (v: unknown): number => Number(v);
    const str = (v: unknown): string => String(v);
    const nstr = (v: unknown): string | null => (v === null || v === undefined ? null : String(v));

    let headRevision = 0;
    for (const { type, known: k } of records) {
      switch (type) {
        case "session":
          headRevision = reqNum(k.headRevision);
          break;
        case "event":
          this.db
            .prepare(
              `INSERT INTO events
                 (session_id, source_id, event_id, segment_id, source_seq, kind, text,
                  start_ms, end_ms, content_hash, received_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
            )
            .run(
              sessionId, str(k.sourceId), str(k.eventId), str(k.segmentId),
              reqNum(k.sourceSeq), str(k.kind), str(k.text), num(k.startMs),
              num(k.endMs), str(k.contentHash), reqNum(k.receivedAt),
            );
          break;
        case "segment":
          this.db
            .prepare(
              `INSERT INTO segments
                 (session_id, source_id, segment_id, kind, text, start_ms, end_ms,
                  source_seq, event_id, finalized, revision, origin, actor)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            )
            .run(
              sessionId, str(k.sourceId), str(k.segmentId), str(k.kind), str(k.text),
              num(k.startMs), num(k.endMs), reqNum(k.sourceSeq), str(k.eventId),
              reqNum(k.finalized), reqNum(k.revision), str(k.origin), nstr(k.actor),
            );
          break;
        case "revision":
          this.db
            .prepare(
              `INSERT INTO revisions
                 (session_id, revision, source_id, segment_id, kind, text, start_ms,
                  end_ms, event_id, source_seq, created_at, origin, actor, reason,
                  correction_id, supersedes_revision)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            )
            .run(
              sessionId, reqNum(k.revision), str(k.sourceId), str(k.segmentId),
              str(k.kind), str(k.text), num(k.startMs), num(k.endMs), str(k.eventId),
              reqNum(k.sourceSeq), reqNum(k.createdAt), str(k.origin), nstr(k.actor),
              nstr(k.reason), nstr(k.correctionId), num(k.supersedesRevision),
            );
          break;
        case "correction":
          this.db
            .prepare(
              `INSERT INTO corrections
                 (session_id, correction_id, source_id, segment_id, actor, reason,
                  text, base_revision, revision, supersedes_revision, lease_id, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
            )
            .run(
              sessionId, str(k.correctionId), str(k.sourceId), str(k.segmentId),
              str(k.actor), str(k.reason), str(k.text), reqNum(k.baseRevision),
              reqNum(k.revision), reqNum(k.supersedesRevision), str(k.leaseId),
              reqNum(k.createdAt),
            );
          break;
        case "lease":
          this.db
            .prepare(
              `INSERT INTO leases
                 (session_id, source_id, segment_id, lease_id, actor, base_revision,
                  acquired_at, expires_at, released)
               VALUES (?,?,?,?,?,?,?,?,?)`,
            )
            .run(
              sessionId, str(k.sourceId), str(k.segmentId), str(k.leaseId), str(k.actor),
              reqNum(k.baseRevision), reqNum(k.acquiredAt), reqNum(k.expiresAt),
              reqNum(k.released),
            );
          break;
        case "consumer":
          this.db
            .prepare(
              `INSERT INTO consumers (session_id, consumer_id, cursor, updated_at)
               VALUES (?,?,?,?)`,
            )
            .run(sessionId, str(k.consumerId), reqNum(k.cursor), reqNum(k.updatedAt));
          break;
        default:
          // Unknown record TYPE (not just unknown fields) is rejected: this
          // version cannot vouch for its meaning under an integrity handoff.
          throw new ArchiveIntegrityError("unknown record type in archive", { type });
      }
    }

    // Rebuild the session's revision allocator from the carried head.
    this.db
      .prepare(
        `INSERT INTO sessions (session_id, head_revision) VALUES (?, ?)
           ON CONFLICT(session_id) DO UPDATE SET head_revision = excluded.head_revision`,
      )
      .run(sessionId, headRevision);
  }

  /** Compute the archive_ext record key for a record of a given type. */
  private extKeyFor(type: string, k: Record<string, unknown>): string {
    switch (type) {
      case "session":
        return "";
      case "event":
        return `${String(k.sourceId)}\u0000${String(k.eventId)}`;
      case "segment":
      case "lease":
        return `${String(k.sourceId)}\u0000${String(k.segmentId)}`;
      case "revision":
        return String(k.revision);
      case "correction":
        return String(k.correctionId);
      case "consumer":
        return String(k.consumerId);
      default:
        return "";
    }
  }

  /** Load persisted ext fields for a session into a lookup map. */
  private loadExt(sessionId: string): Map<string, Record<string, unknown>> {
    const rows = this.db
      .prepare(
        "SELECT record_type, record_key, ext_json FROM archive_ext WHERE session_id=?",
      )
      .all(sessionId) as Array<{ record_type: string; record_key: string; ext_json: string }>;
    const map = new Map<string, Record<string, unknown>>();
    for (const r of rows) {
      map.set(`${r.record_type}\u0000${r.record_key}`, JSON.parse(r.ext_json));
    }
    return map;
  }

  /** Small typed helper around a prepared statement's row iterator. */
  private *iterate(sql: string, ...params: unknown[]): IterableIterator<Record<string, any>> {
    yield* this.db.prepare(sql).iterate(...params) as IterableIterator<Record<string, any>>;
  }
}

/**
 * Normalise an archive (a single string or a stream of arbitrary chunks) into
 * an array of non-empty lines. Chunk boundaries need not align with newlines,
 * so a streamed reader can hand us whatever it read. A trailing newline yields
 * no empty final element.
 */
function collectLines(source: string | Iterable<string>): string[] {
  const out: string[] = [];
  let carry = "";
  const feed = (chunk: string): void => {
    carry += chunk;
    let nl: number;
    while ((nl = carry.indexOf("\n")) >= 0) {
      const line = carry.slice(0, nl);
      carry = carry.slice(nl + 1);
      if (line.length > 0) out.push(line);
    }
  };
  if (typeof source === "string") feed(source);
  else for (const chunk of source) feed(chunk);
  if (carry.length > 0) out.push(carry);
  return out;
}

/** Parse one archive line as a JSON object, failing as an integrity error. */
function parseJson(line: string, what: string): Record<string, unknown> {
  let obj: unknown;
  try {
    obj = JSON.parse(line);
  } catch {
    throw new ArchiveIntegrityError(`malformed ${what}: not valid JSON`);
  }
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
    throw new ArchiveIntegrityError(`malformed ${what}: not an object`);
  }
  return obj as Record<string, unknown>;
}
