import { randomUUID } from 'node:crypto';
import type { Database as DatabaseType } from 'better-sqlite3';
import type { CompactionStats, CheckpointRow } from './types';
import { NoCheckpointError } from './errors';

interface CountRow {
  c: number;
}

interface CursorRow {
  cursor_revision: number;
  lease_expires_at: number | null;
}

export function computeCompactionStats(
  db: DatabaseType,
  sessionId: string,
  now: number = Date.now(),
): {
  safeRevision: number;
  archivedRevision: number;
  slowestActiveCursor: number | null;
  checkpoint: CheckpointRow | null;
} {
  const checkpoint = db
    .prepare(
      `SELECT session_id, checkpoint_id, archive_path, archive_sha256,
              min_revision, max_revision, archived_at, revisions_count, events_count
       FROM compaction_checkpoints
       WHERE session_id = ?
       ORDER BY max_revision DESC
       LIMIT 1`,
    )
    .get(sessionId) as CheckpointRow | undefined;

  const archivedRevision = checkpoint ? checkpoint.max_revision : 0;

  const activeCursors = db
    .prepare(
      `SELECT cursor_revision, lease_expires_at
       FROM consumer_cursors
       WHERE session_id = ? AND lease_expires_at IS NOT NULL AND lease_expires_at > ?`,
    )
    .all(sessionId, now) as CursorRow[];

  let slowestActiveCursor: number | null = null;
  for (const c of activeCursors) {
    if (slowestActiveCursor === null || c.cursor_revision < slowestActiveCursor) {
      slowestActiveCursor = c.cursor_revision;
    }
  }

  let safeRevision: number;
  if (slowestActiveCursor === null) {
    safeRevision = archivedRevision;
  } else {
    safeRevision = Math.min(archivedRevision, slowestActiveCursor);
  }

  return {
    safeRevision,
    archivedRevision,
    slowestActiveCursor,
    checkpoint: checkpoint ?? null,
  };
}

export function compactSession(
  db: DatabaseType,
  sessionId: string,
): CompactionStats {
  const compactTxn = db.transaction((): CompactionStats => {
    const now = Date.now();
    const info = computeCompactionStats(db, sessionId, now);

    const zeroStats: CompactionStats = {
      sessionId,
      compacted: false,
      safeRevision: info.safeRevision,
      archivedRevision: info.archivedRevision,
      slowestActiveCursor: info.slowestActiveCursor,
      revisionsRemoved: 0,
      eventsRemoved: 0,
      revisionsRemaining: (
        db
          .prepare(
            'SELECT COUNT(*) AS c FROM revisions WHERE session_id = ?',
          )
          .get(sessionId) as CountRow
      ).c,
      eventsRemaining: (
        db
          .prepare(
            'SELECT COUNT(*) AS c FROM incoming_events WHERE session_id = ?',
          )
          .get(sessionId) as CountRow
      ).c,
      checkpointId: info.checkpoint?.checkpoint_id ?? null,
      skippedReason: null,
    };

    if (!info.checkpoint) {
      return { ...zeroStats, skippedReason: 'no archive checkpoint exists' };
    }
    if (info.safeRevision <= 0) {
      return { ...zeroStats, skippedReason: 'safe revision is 0' };
    }

    const beforeRevCount = (
      db
        .prepare(
          'SELECT COUNT(*) AS c FROM revisions WHERE session_id = ?',
        )
        .get(sessionId) as CountRow
    ).c;
    const beforeEventCount = (
      db
        .prepare(
          'SELECT COUNT(*) AS c FROM incoming_events WHERE session_id = ?',
        )
        .get(sessionId) as CountRow
    ).c;

    const deleteRevisions = db.prepare(
      `DELETE FROM revisions
       WHERE session_id = ? AND revision <= ?`,
    );
    const deleteEvents = db.prepare(
      `DELETE FROM incoming_events
       WHERE session_id = ? AND resulted_revision <= ?`,
    );

    deleteRevisions.run(sessionId, info.safeRevision);
    deleteEvents.run(sessionId, info.safeRevision);

    const afterRevCount = (
      db
        .prepare(
          'SELECT COUNT(*) AS c FROM revisions WHERE session_id = ?',
        )
        .get(sessionId) as CountRow
    ).c;
    const afterEventCount = (
      db
        .prepare(
          'SELECT COUNT(*) AS c FROM incoming_events WHERE session_id = ?',
        )
        .get(sessionId) as CountRow
    ).c;

    const updateSession = db.prepare(
      `UPDATE sessions
       SET updated_at = ?
       WHERE session_id = ?`,
    );
    updateSession.run(now, sessionId);

    return {
      sessionId,
      compacted: true,
      safeRevision: info.safeRevision,
      archivedRevision: info.archivedRevision,
      slowestActiveCursor: info.slowestActiveCursor,
      revisionsRemoved: beforeRevCount - afterRevCount,
      eventsRemoved: beforeEventCount - afterEventCount,
      revisionsRemaining: afterRevCount,
      eventsRemaining: afterEventCount,
      checkpointId: info.checkpoint.checkpoint_id,
      skippedReason: null,
    };
  });

  return compactTxn.immediate() as CompactionStats;
}

export function recordCheckpoint(
  db: DatabaseType,
  sessionId: string,
  archivePath: string,
  archiveSha256: string,
  minRevision: number,
  maxRevision: number,
  revisionsCount: number,
  eventsCount: number,
): string {
  const checkpointId = randomUUID();
  db.prepare(
    `INSERT INTO compaction_checkpoints
       (session_id, checkpoint_id, archive_path, archive_sha256,
        min_revision, max_revision, archived_at, revisions_count, events_count)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    sessionId,
    checkpointId,
    archivePath,
    archiveSha256,
    minRevision,
    maxRevision,
    Date.now(),
    revisionsCount,
    eventsCount,
  );
  return checkpointId;
}

export function getLatestCheckpoint(
  db: DatabaseType,
  sessionId: string,
) {
  return db
    .prepare(
      `SELECT * FROM compaction_checkpoints
       WHERE session_id = ?
       ORDER BY max_revision DESC LIMIT 1`,
    )
    .get(sessionId) as CheckpointRow | undefined;
}

export function assertCheckpointExists(
  db: DatabaseType,
  sessionId: string,
): void {
  const row = db
    .prepare(
      'SELECT 1 FROM compaction_checkpoints WHERE session_id = ? LIMIT 1',
    )
    .get(sessionId);
  if (!row) {
    throw new NoCheckpointError(
      `no archive checkpoint exists for session '${sessionId}'; create an archive before compacting`,
    );
  }
}
