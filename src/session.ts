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
} from './types';
import { hashEvent } from './hash';
import { buildSnapshotData } from './snapshot';
import { EventConflictError, SlotFinalizedError } from './errors';

const SELECT_FRAGMENTS = `
  SELECT session_id, source_id, source_seq, event_type, content, event_id, updated_at
  FROM fragments
  WHERE session_id = @sessionId
  ORDER BY source_id ASC, source_seq ASC
`;

export class Session {
  private readonly ensureSessionStmt;
  private readonly findEventStmt;
  private readonly insertEventStmt;
  private readonly updateEventRevisionStmt;
  private readonly findFragmentStmt;
  private readonly findMaxSeqStmt;
  private readonly upsertFragmentStmt;
  private readonly insertRevisionStmt;
  private readonly nextRevisionStmt;
  private readonly updateSessionStmt;
  private readonly selectFragmentsStmt;
  private readonly selectRevisionStmt;
  private readonly selectLatestRevisionStmt;
  private readonly ingestTxn: ReturnType<DatabaseType['transaction']>;

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
      `SELECT session_id, source_id, source_seq, event_type, content, event_id, updated_at
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
         (session_id, source_id, source_seq, event_type, content, event_id, updated_at)
       VALUES (@sessionId, @sourceId, @sourceSeq, @eventType, @content, @eventId, @updatedAt)
       ON CONFLICT(session_id, source_id, source_seq) DO UPDATE SET
         event_type = excluded.event_type,
         content    = excluded.content,
         event_id   = excluded.event_id,
         updated_at = excluded.updated_at`,
    );
    this.insertRevisionStmt = db.prepare(
      `INSERT INTO revisions
         (session_id, revision, event_id, source_id, source_seq, change_type,
          content, snapshot_text, summary, created_at)
       VALUES (@sessionId, @revision, @eventId, @sourceId, @sourceSeq, @changeType,
               @content, @snapshotText, @summary, @createdAt)`,
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
      `SELECT session_id, revision, event_id, source_id, source_seq, change_type,
              content, snapshot_text, summary, created_at
       FROM revisions
       WHERE session_id = ? AND revision = ?`,
    );
    this.selectLatestRevisionStmt = db.prepare(
      `SELECT session_id, revision, event_id, source_id, source_seq, change_type,
              content, snapshot_text, summary, created_at
       FROM revisions
       WHERE session_id = ?
       ORDER BY revision DESC
       LIMIT 1`,
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
        eventId: event.eventId,
        sourceId: event.sourceId,
        sourceSeq: event.sourceSeq,
        changeType,
        content: event.content,
        snapshotText: snap.text,
        summary: JSON.stringify(snap.summary),
        createdAt: now,
      });

      this.updateEventRevisionStmt.run(
        revision,
        this.sessionId,
        event.eventId,
      );
      this.updateSessionStmt.run(now, this.sessionId, this.sessionId);

      return { status: 'accepted', revision, reason: null };
    });
  }

  ensureExists(): void {
    const now = Date.now();
    this.ensureSessionStmt.run(this.sessionId, now, now);
  }

  ingest(event: IngestEvent): IngestResult {
    return this.ingestTxn.immediate(event) as IngestResult;
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
    const rows = this.db
      .prepare(
        `SELECT session_id, revision, event_id, source_id, source_seq, change_type,
                content, snapshot_text, summary, created_at
         FROM revisions
         WHERE session_id = ? AND revision > ?
         ORDER BY revision ASC
         LIMIT ?`,
      )
      .all(this.sessionId, cursor, limit) as RevisionRow[];
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
  };
}
