import Database from 'better-sqlite3';
import type { Database as DatabaseType } from 'better-sqlite3';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

const SCHEMA_VERSION = 2;

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
  session_id   TEXT NOT NULL,
  source_id    TEXT NOT NULL,
  source_seq   INTEGER NOT NULL,
  event_type   TEXT NOT NULL,
  content      TEXT NOT NULL,
  event_id     TEXT NOT NULL,
  is_corrected INTEGER NOT NULL DEFAULT 0,
  updated_at   INTEGER NOT NULL,
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
  correction_id TEXT,
  metadata      TEXT,
  created_at    INTEGER NOT NULL,
  PRIMARY KEY (session_id, revision),
  FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS consumer_cursors (
  session_id       TEXT NOT NULL,
  consumer_id      TEXT NOT NULL,
  cursor_revision  INTEGER NOT NULL DEFAULT 0,
  updated_at       INTEGER NOT NULL,
  lease_expires_at INTEGER,
  PRIMARY KEY (session_id, consumer_id),
  FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS compaction_checkpoints (
  session_id      TEXT NOT NULL,
  checkpoint_id   TEXT NOT NULL,
  archive_path    TEXT NOT NULL,
  archive_sha256  TEXT NOT NULL,
  min_revision    INTEGER NOT NULL,
  max_revision    INTEGER NOT NULL,
  archived_at     INTEGER NOT NULL,
  revisions_count INTEGER NOT NULL,
  events_count    INTEGER NOT NULL,
  PRIMARY KEY (session_id, checkpoint_id),
  FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS correction_leases (
  session_id    TEXT NOT NULL,
  source_id     TEXT NOT NULL,
  source_seq    INTEGER NOT NULL,
  lease_id      TEXT NOT NULL,
  actor         TEXT NOT NULL,
  base_revision INTEGER NOT NULL,
  base_content  TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'active',
  acquired_at   INTEGER NOT NULL,
  expires_at    INTEGER NOT NULL,
  consumed_at   INTEGER,
  PRIMARY KEY (session_id, source_id, source_seq),
  UNIQUE (session_id, lease_id),
  FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS corrections (
  session_id                TEXT NOT NULL,
  correction_id             TEXT NOT NULL,
  lease_id                  TEXT NOT NULL,
  source_id                 TEXT NOT NULL,
  source_seq                INTEGER NOT NULL,
  actor                     TEXT NOT NULL,
  reason                    TEXT NOT NULL,
  original_content          TEXT NOT NULL,
  corrected_content         TEXT NOT NULL,
  supersedes_correction_id  TEXT,
  revision                  INTEGER NOT NULL,
  created_at                INTEGER NOT NULL,
  PRIMARY KEY (session_id, correction_id),
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
CREATE INDEX IF NOT EXISTS idx_corrections_fragment
  ON corrections (session_id, source_id, source_seq);
CREATE INDEX IF NOT EXISTS idx_leases_lease_id
  ON correction_leases (session_id, lease_id);
`;

function addColumnIfMissing(
  db: DatabaseType,
  table: string,
  column: string,
  definition: string,
): void {
  const columns = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{
    name: string;
  }>;
  if (!columns.some((c) => c.name === column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
  }
}

function runMigrations(db: DatabaseType): void {
  addColumnIfMissing(
    db,
    'fragments',
    'is_corrected',
    'INTEGER NOT NULL DEFAULT 0',
  );
  addColumnIfMissing(db, 'revisions', 'correction_id', 'TEXT');
  addColumnIfMissing(db, 'revisions', 'metadata', 'TEXT');
  addColumnIfMissing(db, 'consumer_cursors', 'lease_expires_at', 'INTEGER');
}

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
  runMigrations(db);

  db.prepare(
    `INSERT OR IGNORE INTO schema_meta (key, value) VALUES (?, ?)`,
  ).run('version', String(SCHEMA_VERSION));

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
