import type { DB } from "./db";
import { openDb } from "./db";
import { contentHash } from "./hash";
import { ConflictError, ValidationError } from "./errors";
import type {
  ApplyResult,
  ApplyOutcome,
  PullOptions,
  RevisionHubOptions,
  RevisionRecord,
  SegmentState,
  SessionSummary,
  Snapshot,
  SourceEvent,
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
}

interface EventRow {
  content_hash: string;
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
  private closed = false;

  constructor(opts: RevisionHubOptions) {
    this.db = openDb(opts);
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
      const now = Date.now();
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
                  event_id, finalized, revision
             FROM segments
            WHERE session_id=? AND source_id=? AND segment_id=?`,
        )
        .get(ev.sessionId, ev.sourceId, ev.segmentId) as SegmentRow | undefined;

      let outcome: ApplyOutcome;
      if (!seg) {
        outcome = "created";
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
      this.db
        .prepare(
          `INSERT INTO sessions (session_id, head_revision) VALUES (?, 1)
             ON CONFLICT(session_id) DO UPDATE SET head_revision = head_revision + 1`,
        )
        .run(ev.sessionId);
      const revision = (
        this.db
          .prepare("SELECT head_revision AS r FROM sessions WHERE session_id=?")
          .get(ev.sessionId) as { r: number }
      ).r;

      // 5) Upsert resolved segment state.
      this.db
        .prepare(
          `INSERT INTO segments
             (session_id, source_id, segment_id, kind, text, start_ms, end_ms,
              source_seq, event_id, finalized, revision)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(session_id, source_id, segment_id) DO UPDATE SET
             kind=excluded.kind, text=excluded.text, start_ms=excluded.start_ms,
             end_ms=excluded.end_ms, source_seq=excluded.source_seq,
             event_id=excluded.event_id, finalized=excluded.finalized,
             revision=excluded.revision`,
        )
        .run(
          ev.sessionId,
          ev.sourceId,
          ev.segmentId,
          ev.kind,
          ev.text,
          startMs,
          endMs,
          ev.sourceSeq,
          ev.eventId,
          ev.kind === "final" ? 1 : 0,
          revision,
        );

      // 6) Append to the durable revision stream.
      this.db
        .prepare(
          `INSERT INTO revisions
             (session_id, revision, source_id, segment_id, kind, text, start_ms,
              end_ms, event_id, source_seq, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
        )
        .run(
          ev.sessionId,
          revision,
          ev.sourceId,
          ev.segmentId,
          ev.kind,
          ev.text,
          startMs,
          endMs,
          ev.eventId,
          ev.sourceSeq,
          now,
        );

      return this.result(ev, outcome, revision);
    });

    // IMMEDIATE takes the write lock up front, serialising writers across
    // processes so revision allocation is race-free.
    return runner.immediate();
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
                event_id, revision
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
                end_ms, event_id, source_seq, created_at
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
      const now = Date.now();
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
        .run(sessionId, consumerId, Date.now());
    }
    return 0;
  }
}
