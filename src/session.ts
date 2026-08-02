import { randomUUID } from 'node:crypto';
import type { Database as DatabaseType } from 'better-sqlite3';
import type {
  IngestEvent,
  IngestResult,
  Snapshot,
  FragmentRow,
  IncomingEventRow,
  RevisionRow,
  RevisionData,
  SessionSummary,
  LeaseRow,
  Lease,
  CorrectionRow,
  Correction,
  AcquireLeaseOptions,
  SubmitCorrectionOptions,
} from './types';
import { hashEvent } from './hash';
import { buildSnapshotData } from './snapshot';
import {
  EventConflictError,
  SlotFinalizedError,
  LeaseBusyError,
  LeaseExpiredError,
  LeaseConsumedError,
  LeaseNotFoundError,
  StaleBaseRevisionError,
} from './errors';

const SELECT_FRAGMENTS = `
  SELECT session_id, source_id, source_seq, event_type, content, event_id,
         is_corrected, updated_at
  FROM fragments
  WHERE session_id = @sessionId
  ORDER BY source_id ASC, source_seq ASC
`;

const SELECT_REVISION_COLS = `
  session_id, revision, event_id, source_id, source_seq, change_type,
  content, snapshot_text, summary, created_at, correction_id, metadata
`;

export class Session {
  private readonly ensureSessionStmt;
  private readonly findEventStmt;
  private readonly insertEventStmt;
  private readonly updateEventRevisionStmt;
  private readonly findFragmentStmt;
  private readonly findMaxSeqStmt;
  private readonly upsertFragmentStmt;
  private readonly updateFragmentCorrectedStmt;
  private readonly insertRevisionStmt;
  private readonly nextRevisionStmt;
  private readonly updateSessionStmt;
  private readonly selectFragmentsStmt;
  private readonly selectRevisionStmt;
  private readonly selectLatestRevisionStmt;
  private readonly selectRevisionsSinceStmt;
  private readonly findLeaseByLocationStmt;
  private readonly findLeaseByIdStmt;
  private readonly upsertLeaseStmt;
  private readonly consumeLeaseStmt;
  private readonly releaseLeaseStmt;
  private readonly expireLeaseStmt;
  private readonly findLastCorrectionStmt;
  private readonly insertCorrectionStmt;
  private readonly selectCorrectionsForFragmentStmt;
  private readonly ingestTxn: ReturnType<DatabaseType['transaction']>;
  private readonly acquireLeaseTxn: ReturnType<DatabaseType['transaction']>;
  private readonly tryExpireLeaseTxn: ReturnType<DatabaseType['transaction']>;
  private readonly submitCorrectionTxn: ReturnType<DatabaseType['transaction']>;
  private readonly releaseLeaseTxn: ReturnType<DatabaseType['transaction']>;

  constructor(
    private readonly db: DatabaseType,
    public readonly sessionId: string,
  ) {
    this.ensureSessionStmt = db.prepare(
      `INSERT OR IGNORE INTO sessions (session_id, created_at, updated_at, latest_revision)
       VALUES (?, ?, ?, 0)`,
    );
    this.findEventStmt = db.prepare(
      `SELECT session_id, event_id, source_id, source_seq, event_type, content,
              content_hash, resulted_revision, received_at
       FROM incoming_events
       WHERE session_id = ? AND event_id = ?`,
    );
    this.insertEventStmt = db.prepare(
      `INSERT INTO incoming_events
         (session_id, event_id, source_id, source_seq, event_type, content,
          content_hash, resulted_revision, received_at)
       VALUES (@sessionId, @eventId, @sourceId, @sourceSeq, @eventType, @content,
               @contentHash, NULL, @receivedAt)`,
    );
    this.updateEventRevisionStmt = db.prepare(
      `UPDATE incoming_events SET resulted_revision = ?
       WHERE session_id = ? AND event_id = ?`,
    );
    this.findFragmentStmt = db.prepare(
      `SELECT session_id, source_id, source_seq, event_type, content, event_id,
              is_corrected, updated_at
       FROM fragments
       WHERE session_id = ? AND source_id = ? AND source_seq = ?`,
    );
    this.findMaxSeqStmt = db.prepare(
      `SELECT COALESCE(MAX(source_seq), -1) AS max_seq
       FROM fragments
       WHERE session_id = ? AND source_id = ?`,
    );
    this.upsertFragmentStmt = db.prepare(
      `INSERT INTO fragments
         (session_id, source_id, source_seq, event_type, content, event_id,
          is_corrected, updated_at)
       VALUES (@sessionId, @sourceId, @sourceSeq, @eventType, @content, @eventId,
               0, @updatedAt)
       ON CONFLICT(session_id, source_id, source_seq) DO UPDATE SET
         event_type   = excluded.event_type,
         content      = excluded.content,
         event_id     = excluded.event_id,
         is_corrected = 0,
         updated_at   = excluded.updated_at`,
    );
    this.updateFragmentCorrectedStmt = db.prepare(
      `UPDATE fragments
       SET content = @content, is_corrected = 1, event_id = @eventId,
           updated_at = @updatedAt
       WHERE session_id = @sessionId AND source_id = @sourceId
         AND source_seq = @sourceSeq`,
    );
    this.insertRevisionStmt = db.prepare(
      `INSERT INTO revisions
         (session_id, revision, event_id, source_id, source_seq, change_type,
          content, snapshot_text, summary, correction_id, metadata, created_at)
       VALUES (@sessionId, @revision, @eventId, @sourceId, @sourceSeq,
               @changeType, @content, @snapshotText, @summary,
               @correctionId, @metadata, @createdAt)`,
    );
    this.nextRevisionStmt = db.prepare(
      `SELECT COALESCE(MAX(revision), 0) + 1 AS next_rev
       FROM revisions WHERE session_id = ?`,
    );
    this.updateSessionStmt = db.prepare(
      `UPDATE sessions SET updated_at = ?, latest_revision =
         (SELECT MAX(revision) FROM revisions WHERE session_id = ?)
       WHERE session_id = ?`,
    );
    this.selectFragmentsStmt = db.prepare(SELECT_FRAGMENTS);
    this.selectRevisionStmt = db.prepare(
      `SELECT ${SELECT_REVISION_COLS}
       FROM revisions
       WHERE session_id = ? AND revision = ?`,
    );
    this.selectLatestRevisionStmt = db.prepare(
      `SELECT ${SELECT_REVISION_COLS}
       FROM revisions
       WHERE session_id = ?
       ORDER BY revision DESC
       LIMIT 1`,
    );
    this.selectRevisionsSinceStmt = db.prepare(
      `SELECT ${SELECT_REVISION_COLS}
       FROM revisions
       WHERE session_id = ? AND revision > ?
       ORDER BY revision ASC
       LIMIT ?`,
    );
    this.findLeaseByLocationStmt = db.prepare(
      `SELECT session_id, source_id, source_seq, lease_id, actor, base_revision,
              base_content, status, acquired_at, expires_at, consumed_at
       FROM correction_leases
       WHERE session_id = ? AND source_id = ? AND source_seq = ?`,
    );
    this.findLeaseByIdStmt = db.prepare(
      `SELECT session_id, source_id, source_seq, lease_id, actor, base_revision,
              base_content, status, acquired_at, expires_at, consumed_at
       FROM correction_leases
       WHERE session_id = ? AND lease_id = ?`,
    );
    this.upsertLeaseStmt = db.prepare(
      `INSERT INTO correction_leases
         (session_id, source_id, source_seq, lease_id, actor, base_revision,
          base_content, status, acquired_at, expires_at, consumed_at)
       VALUES (@sessionId, @sourceId, @sourceSeq, @leaseId, @actor,
               @baseRevision, @baseContent, 'active', @acquiredAt, @expiresAt,
               NULL)
       ON CONFLICT(session_id, source_id, source_seq) DO UPDATE SET
         lease_id      = excluded.lease_id,
         actor         = excluded.actor,
         base_revision = excluded.base_revision,
         base_content  = excluded.base_content,
         status        = 'active',
         acquired_at   = excluded.acquired_at,
         expires_at    = excluded.expires_at,
         consumed_at   = NULL`,
    );
    this.consumeLeaseStmt = db.prepare(
      `UPDATE correction_leases SET status = 'consumed', consumed_at = ?
       WHERE session_id = ? AND lease_id = ?`,
    );
    this.releaseLeaseStmt = db.prepare(
      `UPDATE correction_leases SET status = 'released'
       WHERE session_id = ? AND lease_id = ? AND status = 'active'`,
    );
    this.expireLeaseStmt = db.prepare(
      `UPDATE correction_leases SET status = 'expired'
       WHERE session_id = ? AND source_id = ? AND source_seq = ?
         AND status = 'active'`,
    );
    this.findLastCorrectionStmt = db.prepare(
      `SELECT session_id, correction_id, lease_id, source_id, source_seq, actor,
              reason, original_content, corrected_content,
              supersedes_correction_id, revision, created_at
       FROM corrections
       WHERE session_id = ? AND source_id = ? AND source_seq = ?
       ORDER BY created_at DESC, revision DESC
       LIMIT 1`,
    );
    this.insertCorrectionStmt = db.prepare(
      `INSERT INTO corrections
         (session_id, correction_id, lease_id, source_id, source_seq, actor,
          reason, original_content, corrected_content,
          supersedes_correction_id, revision, created_at)
       VALUES (@sessionId, @correctionId, @leaseId, @sourceId, @sourceSeq,
               @actor, @reason, @originalContent, @correctedContent,
               @supersedesCorrectionId, @revision, @createdAt)`,
    );
    this.selectCorrectionsForFragmentStmt = db.prepare(
      `SELECT session_id, correction_id, lease_id, source_id, source_seq, actor,
              reason, original_content, corrected_content,
              supersedes_correction_id, revision, created_at
       FROM corrections
       WHERE session_id = ? AND source_id = ? AND source_seq = ?
       ORDER BY revision ASC`,
    );

    this.ingestTxn = db.transaction((event: IngestEvent): IngestResult => {
      const now = Date.now();
      const contentHash = hashEvent(event);

      this.ensureSessionStmt.run(this.sessionId, now, now);

      const existing = this.findEventStmt.get(
        this.sessionId,
        event.eventId,
      ) as IncomingEventRow | undefined;

      if (existing) {
        if (existing.content_hash !== contentHash) {
          throw new EventConflictError(
            `eventId '${event.eventId}' was already ingested with different content`,
          );
        }
        return {
          status: 'duplicate',
          revision: existing.resulted_revision,
          reason: 'duplicate eventId',
        };
      }

      this.insertEventStmt.run({
        sessionId: this.sessionId,
        eventId: event.eventId,
        sourceId: event.sourceId,
        sourceSeq: event.sourceSeq,
        eventType: event.type,
        content: event.content,
        contentHash,
        receivedAt: now,
      });

      const currentFragment = this.findFragmentStmt.get(
        this.sessionId,
        event.sourceId,
        event.sourceSeq,
      ) as FragmentRow | undefined;

      let shouldUpdate = false;
      let reason: string | null = null;
      const changeType =
        event.type === 'final' ? 'final-committed' : 'partial-updated';

      if (event.type === 'final') {
        if (currentFragment && currentFragment.is_corrected === 1) {
          throw new SlotFinalizedError(
            `source '${event.sourceId}' seq ${event.sourceSeq} has been human-corrected and is locked`,
          );
        }
        if (currentFragment && currentFragment.event_type === 'final') {
          throw new SlotFinalizedError(
            `source '${event.sourceId}' seq ${event.sourceSeq} is already finalized`,
          );
        }
        shouldUpdate = true;
      } else {
        const maxSeqRow = this.findMaxSeqStmt.get(
          this.sessionId,
          event.sourceId,
        ) as { max_seq: number };
        const maxSeq = maxSeqRow.max_seq;

        if (event.sourceSeq < maxSeq) {
          shouldUpdate = false;
          reason = 'partial rejected: older than current state';
        } else if (
          currentFragment &&
          currentFragment.is_corrected === 1
        ) {
          shouldUpdate = false;
          reason = 'partial rejected: fragment has been human-corrected';
        } else if (
          currentFragment &&
          currentFragment.event_type === 'final'
        ) {
          shouldUpdate = false;
          reason = 'partial rejected: slot already finalized';
        } else if (
          currentFragment &&
          currentFragment.content === event.content
        ) {
          shouldUpdate = false;
          reason = 'partial unchanged';
        } else {
          shouldUpdate = true;
        }
      }

      if (!shouldUpdate) {
        this.updateSessionStmt.run(now, this.sessionId, this.sessionId);
        return { status: 'ignored', revision: null, reason };
      }

      this.upsertFragmentStmt.run({
        sessionId: this.sessionId,
        sourceId: event.sourceId,
        sourceSeq: event.sourceSeq,
        eventType: event.type,
        content: event.content,
        eventId: event.eventId,
        updatedAt: now,
      });

      const revision = this.insertRevisionAndSnapshot(
        event.eventId,
        event.sourceId,
        event.sourceSeq,
        changeType,
        event.content,
        now,
        null,
        null,
      );

      this.updateEventRevisionStmt.run(
        revision,
        this.sessionId,
        event.eventId,
      );
      this.updateSessionStmt.run(now, this.sessionId, this.sessionId);

      return { status: 'accepted', revision, reason: null };
    });

    this.acquireLeaseTxn = db.transaction(
      (options: AcquireLeaseOptions): Lease => {
        const now = Date.now();
        this.ensureSessionStmt.run(this.sessionId, now, now);

        const fragment = this.findFragmentStmt.get(
          this.sessionId,
          options.sourceId,
          options.sourceSeq,
        ) as FragmentRow | undefined;

        if (!fragment) {
          throw new LeaseNotFoundError(
            `fragment source='${options.sourceId}' seq=${options.sourceSeq} does not exist`,
          );
        }

        const existing = this.findLeaseByLocationStmt.get(
          this.sessionId,
          options.sourceId,
          options.sourceSeq,
        ) as LeaseRow | undefined;

        if (existing && existing.status === 'active') {
          if (existing.expires_at > now) {
            throw new LeaseBusyError(
              `an active lease already exists for source='${options.sourceId}' seq=${options.sourceSeq} held by '${existing.actor}'`,
            );
          }
          this.expireLeaseStmt.run(
            this.sessionId,
            options.sourceId,
            options.sourceSeq,
          );
        }

        const ttl = options.ttlMs ?? 60000;
        const leaseId = randomUUID();
        const acquiredAt = now;
        const expiresAt = now + ttl;

        this.upsertLeaseStmt.run({
          sessionId: this.sessionId,
          sourceId: options.sourceId,
          sourceSeq: options.sourceSeq,
          leaseId,
          actor: options.actor,
          baseRevision: options.baseRevision,
          baseContent: fragment.content,
          acquiredAt,
          expiresAt,
        });

        return {
          leaseId,
          sessionId: this.sessionId,
          sourceId: options.sourceId,
          sourceSeq: options.sourceSeq,
          actor: options.actor,
          baseRevision: options.baseRevision,
          baseContent: fragment.content,
          status: 'active',
          acquiredAt,
          expiresAt,
          consumedAt: null,
        };
      },
    );

    this.tryExpireLeaseTxn = db.transaction((leaseId: string): void => {
      const row = this.findLeaseByIdStmt.get(
        this.sessionId,
        leaseId,
      ) as LeaseRow | undefined;
      if (row && row.status === 'active' && row.expires_at <= Date.now()) {
        this.expireLeaseStmt.run(
          this.sessionId,
          row.source_id,
          row.source_seq,
        );
      }
    });

    this.submitCorrectionTxn = db.transaction(
      (options: SubmitCorrectionOptions): Correction => {
        const now = Date.now();
        const leaseRow = this.findLeaseByIdStmt.get(
          this.sessionId,
          options.leaseId,
        ) as LeaseRow | undefined;

        if (!leaseRow) {
          throw new LeaseNotFoundError(
            `lease '${options.leaseId}' not found`,
          );
        }

        if (leaseRow.status === 'consumed') {
          throw new LeaseConsumedError(
            `lease '${options.leaseId}' has already been consumed`,
          );
        }
        if (leaseRow.status === 'released') {
          throw new LeaseNotFoundError(
            `lease '${options.leaseId}' was released`,
          );
        }
        if (leaseRow.status === 'expired') {
          throw new LeaseExpiredError(
            `lease '${options.leaseId}' has expired`,
          );
        }
        if (leaseRow.expires_at <= now) {
          throw new LeaseExpiredError(
            `lease '${options.leaseId}' expired at ${leaseRow.expires_at}`,
          );
        }

        const fragment = this.findFragmentStmt.get(
          this.sessionId,
          leaseRow.source_id,
          leaseRow.source_seq,
        ) as FragmentRow | undefined;

        if (!fragment) {
          throw new LeaseNotFoundError(
            `target fragment disappeared for lease '${options.leaseId}'`,
          );
        }

        if (fragment.content !== leaseRow.base_content) {
          throw new StaleBaseRevisionError(
            `fragment content changed since lease '${options.leaseId}' was acquired; ` +
              `base was ${JSON.stringify(leaseRow.base_content)} but current is ${JSON.stringify(fragment.content)}`,
          );
        }

        const previousCorrection = this.findLastCorrectionStmt.get(
          this.sessionId,
          leaseRow.source_id,
          leaseRow.source_seq,
        ) as CorrectionRow | undefined;

        const correctionId = randomUUID();
        const originalContent = leaseRow.base_content;
        const supersedesCorrectionId = previousCorrection
          ? previousCorrection.correction_id
          : null;

        this.updateFragmentCorrectedStmt.run({
          sessionId: this.sessionId,
          sourceId: leaseRow.source_id,
          sourceSeq: leaseRow.source_seq,
          content: options.correctedContent,
          eventId: fragment.event_id,
          updatedAt: now,
        });

        const metadata = {
          actor: leaseRow.actor,
          reason: options.reason,
          originalContent,
          supersedesCorrectionId,
        };

        const revision = this.insertRevisionAndSnapshot(
          fragment.event_id,
          leaseRow.source_id,
          leaseRow.source_seq,
          'correction-applied',
          options.correctedContent,
          now,
          correctionId,
          JSON.stringify(metadata),
        );

        this.insertCorrectionStmt.run({
          sessionId: this.sessionId,
          correctionId,
          leaseId: options.leaseId,
          sourceId: leaseRow.source_id,
          sourceSeq: leaseRow.source_seq,
          actor: leaseRow.actor,
          reason: options.reason,
          originalContent,
          correctedContent: options.correctedContent,
          supersedesCorrectionId,
          revision,
          createdAt: now,
        });

        this.consumeLeaseStmt.run(now, this.sessionId, options.leaseId);
        this.updateSessionStmt.run(now, this.sessionId, this.sessionId);

        return {
          correctionId,
          leaseId: options.leaseId,
          sessionId: this.sessionId,
          sourceId: leaseRow.source_id,
          sourceSeq: leaseRow.source_seq,
          actor: leaseRow.actor,
          reason: options.reason,
          originalContent,
          correctedContent: options.correctedContent,
          supersedesCorrectionId,
          revision,
          createdAt: now,
        };
      },
    );

    this.releaseLeaseTxn = db.transaction((leaseId: string): boolean => {
      const info = this.releaseLeaseStmt.run(this.sessionId, leaseId);
      return info.changes > 0;
    });
  }

  private insertRevisionAndSnapshot(
    eventId: string,
    sourceId: string,
    sourceSeq: number,
    changeType: RevisionData['changeType'],
    content: string,
    now: number,
    correctionId: string | null,
    metadata: string | null,
  ): number {
    const fragmentRows = this.selectFragmentsStmt.all({
      sessionId: this.sessionId,
    }) as FragmentRow[];
    const snap = buildSnapshotData(fragmentRows);

    const nextRevRow = this.nextRevisionStmt.get(this.sessionId) as {
      next_rev: number;
    };
    const revision = nextRevRow.next_rev;

    this.insertRevisionStmt.run({
      sessionId: this.sessionId,
      revision,
      eventId,
      sourceId,
      sourceSeq,
      changeType,
      content,
      snapshotText: snap.text,
      summary: JSON.stringify(snap.summary),
      correctionId,
      metadata,
      createdAt: now,
    });

    return revision;
  }

  ensureExists(): void {
    const now = Date.now();
    this.ensureSessionStmt.run(this.sessionId, now, now);
  }

  ingest(event: IngestEvent): IngestResult {
    return this.ingestTxn.immediate(event) as IngestResult;
  }

  acquireLease(options: AcquireLeaseOptions): Lease {
    return this.acquireLeaseTxn.immediate(options) as Lease;
  }

  submitCorrection(options: SubmitCorrectionOptions): Correction {
    this.tryExpireLeaseTxn.immediate(options.leaseId);
    return this.submitCorrectionTxn.immediate(options) as Correction;
  }

  releaseLease(leaseId: string): boolean {
    return this.releaseLeaseTxn.immediate(leaseId) as boolean;
  }

  getLease(sourceId: string, sourceSeq: number): Lease | null {
    this.ensureExists();
    const row = this.findLeaseByLocationStmt.get(
      this.sessionId,
      sourceId,
      sourceSeq,
    ) as LeaseRow | undefined;
    if (!row) return null;
    if (row.status === 'active' && row.expires_at <= Date.now()) {
      this.expireLeaseStmt.run(this.sessionId, sourceId, sourceSeq);
      row.status = 'expired';
    }
    return rowToLease(row);
  }

  getCorrectionsForFragment(
    sourceId: string,
    sourceSeq: number,
  ): Correction[] {
    this.ensureExists();
    const rows = this.selectCorrectionsForFragmentStmt.all(
      this.sessionId,
      sourceId,
      sourceSeq,
    ) as CorrectionRow[];
    return rows.map(rowToCorrection);
  }

  getSnapshot(): Snapshot {
    this.ensureExists();
    const rows = this.selectFragmentsStmt.all({
      sessionId: this.sessionId,
    }) as FragmentRow[];
    const snap = buildSnapshotData(rows);

    const sessionRow = this.db
      .prepare(
        'SELECT latest_revision FROM sessions WHERE session_id = ?',
      )
      .get(this.sessionId) as { latest_revision: number } | undefined;
    const revision = sessionRow ? sessionRow.latest_revision : 0;

    return {
      sessionId: this.sessionId,
      revision,
      sources: snap.sources,
      text: snap.text,
      summary: snap.summary,
    };
  }

  getRevision(revision: number): RevisionData | null {
    const row = this.selectRevisionStmt.get(
      this.sessionId,
      revision,
    ) as RevisionRow | undefined;
    return row ? rowToRevision(row) : null;
  }

  getLatestRevision(): RevisionData | null {
    const row = this.selectLatestRevisionStmt.get(
      this.sessionId,
    ) as RevisionRow | undefined;
    return row ? rowToRevision(row) : null;
  }

  getRevisionsSince(cursor: number, limit: number): RevisionData[] {
    const rows = this.selectRevisionsSinceStmt.all(
      this.sessionId,
      cursor,
      limit,
    ) as RevisionRow[];
    return rows.map(rowToRevision);
  }

  getLatestRevisionNumber(): number {
    const row = this.db
      .prepare(
        'SELECT latest_revision FROM sessions WHERE session_id = ?',
      )
      .get(this.sessionId) as { latest_revision: number } | undefined;
    return row ? row.latest_revision : 0;
  }
}

function rowToRevision(row: RevisionRow): RevisionData {
  let correction: RevisionData['correction'] = null;
  if (row.correction_id && row.metadata) {
    const meta = JSON.parse(row.metadata) as {
      actor: string;
      reason: string;
      originalContent: string;
      supersedesCorrectionId: string | null;
    };
    correction = meta;
  }
  return {
    sessionId: row.session_id,
    revision: row.revision,
    eventId: row.event_id,
    sourceId: row.source_id,
    sourceSeq: row.source_seq,
    changeType: row.change_type,
    content: row.content,
    snapshotText: row.snapshot_text,
    summary: JSON.parse(row.summary) as SessionSummary,
    createdAt: row.created_at,
    correctionId: row.correction_id ?? null,
    correction,
  };
}

function rowToLease(row: LeaseRow): Lease {
  return {
    leaseId: row.lease_id,
    sessionId: row.session_id,
    sourceId: row.source_id,
    sourceSeq: row.source_seq,
    actor: row.actor,
    baseRevision: row.base_revision,
    baseContent: row.base_content,
    status: row.status,
    acquiredAt: row.acquired_at,
    expiresAt: row.expires_at,
    consumedAt: row.consumed_at,
  };
}

function rowToCorrection(row: CorrectionRow): Correction {
  return {
    correctionId: row.correction_id,
    leaseId: row.lease_id,
    sessionId: row.session_id,
    sourceId: row.source_id,
    sourceSeq: row.source_seq,
    actor: row.actor,
    reason: row.reason,
    originalContent: row.original_content,
    correctedContent: row.corrected_content,
    supersedesCorrectionId: row.supersedes_correction_id,
    revision: row.revision,
    createdAt: row.created_at,
  };
}
