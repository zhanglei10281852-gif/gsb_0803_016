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
} from "./errors";
import type {
  AcquireLeaseRequest,
  ApplyResult,
  ApplyOutcome,
  CorrectionResult,
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
}
