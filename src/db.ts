import Database from 'better-sqlite3';
import type { Database as DatabaseType } from 'better-sqlite3';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

const SCHEMA_VERSION = 1;

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS schema_meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  session_id      TEXT PRIMARY KEY,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  latest_revision INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS incoming_events (
  session_id        TEXT NOT NULL,
  event_id          TEXT NOT NULL,
  source_id         TEXT NOT NULL,
  source_seq        INTEGER NOT NULL,
  event_type        TEXT NOT NULL,
  content           TEXT NOT NULL,
  content_hash      TEXT NOT NULL,
  resulted_revision INTEGER,
  received_at       INTEGER NOT NULL,
  PRIMARY KEY (session_id, event_id),
  FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS fragments (
  session_id  TEXT NOT NULL,
  source_id   TEXT NOT NULL,
  source_seq  INTEGER NOT NULL,
  event_type  TEXT NOT NULL,
  content     TEXT NOT NULL,
  event_id    TEXT NOT NULL,
  updated_at  INTEGER NOT NULL,
  PRIMARY KEY (session_id, source_id, source_seq),
  FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS revisions (
  session_id    TEXT NOT NULL,
  revision      INTEGER NOT NULL,
  event_id      TEXT NOT NULL,
  source_id     TEXT NOT NULL,
  source_seq    INTEGER NOT NULL,
  change_type   TEXT NOT NULL,
  content       TEXT NOT NULL,
  snapshot_text TEXT NOT NULL,
  summary       TEXT NOT NULL,
  created_at    INTEGER NOT NULL,
  PRIMARY KEY (session_id, revision),
  FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS consumer_cursors (
  session_id      TEXT NOT NULL,
  consumer_id     TEXT NOT NULL,
  cursor_revision INTEGER NOT NULL DEFAULT 0,
  updated_at      INTEGER NOT NULL,
  PRIMARY KEY (session_id, consumer_id),
  FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE INDEX IF NOT EXISTS idx_incoming_events_source
  ON incoming_events (session_id, source_id, source_seq);
CREATE INDEX IF NOT EXISTS idx_fragments_lookup
  ON fragments (session_id, source_id, source_seq);
CREATE INDEX IF NOT EXISTS idx_revisions_session
  ON revisions (session_id, revision);
CREATE INDEX IF NOT EXISTS idx_cursors_consumer
  ON consumer_cursors (session_id, consumer_id);
`;

export interface OpenDbResult {
  db: DatabaseType;
  close: () => void;
}

export function openDatabase(
  filename: string,
  busyTimeoutMs = 10000,
): OpenDbResult {
  const dir = dirname(filename);
  if (dir && dir !== '.') {
    mkdirSync(dir, { recursive: true });
  }

  const db = new Database(filename);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = FULL');
  db.pragma('foreign_keys = ON');
  db.pragma(`busy_timeout = ${busyTimeoutMs}`);

  db.exec(SCHEMA_SQL);

  const versionRow = db
    .prepare('SELECT value FROM schema_meta WHERE key = ?')
    .get('version') as { value: string } | undefined;
  if (!versionRow) {
    db.prepare(
      'INSERT OR IGNORE INTO schema_meta (key, value) VALUES (?, ?)',
    ).run('version', String(SCHEMA_VERSION));
  }

  const close = () => {
    try {
      db.prepare('PRAGMA wal_checkpoint(TRUNCATE)').run();
    } catch {
      // checkpoint is best-effort on close
    }
    db.close();
  };

  return { db, close };
}
