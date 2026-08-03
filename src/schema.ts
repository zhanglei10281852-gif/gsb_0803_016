import type Database from "better-sqlite3";

export const SCHEMA_VERSION = 3;

export const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS events (
  session_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  source_seq INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK(kind IN ('final', 'partial')),
  text TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(session_id, event_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_source_kind_seq
  ON events(session_id, source_id, kind, source_seq);

CREATE INDEX IF NOT EXISTS idx_events_session_source
  ON events(session_id, source_id);

CREATE TABLE IF NOT EXISTS revisions (
  session_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  change_type TEXT NOT NULL DEFAULT 'event' CHECK(change_type IN ('event', 'correction')),
  event_id TEXT,
  source_id TEXT,
  source_seq INTEGER,
  kind TEXT,
  correction_id TEXT,
  snapshot TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(session_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_revisions_event
  ON revisions(session_id, event_id);

CREATE INDEX IF NOT EXISTS idx_revisions_correction
  ON revisions(session_id, correction_id);

CREATE TABLE IF NOT EXISTS consumer_cursors (
  session_id TEXT NOT NULL,
  consumer_id TEXT NOT NULL,
  cursor INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(session_id, consumer_id)
);

CREATE TABLE IF NOT EXISTS review_leases (
  session_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  source_seq INTEGER NOT NULL,
  lease_id TEXT NOT NULL,
  actor TEXT NOT NULL,
  base_revision INTEGER NOT NULL,
  ttl_ms INTEGER NOT NULL,
  claimed_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  released_at INTEGER,
  PRIMARY KEY(session_id, source_id, source_seq)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_review_leases_lease_id
  ON review_leases(session_id, lease_id);

CREATE TABLE IF NOT EXISTS corrections (
  session_id TEXT NOT NULL,
  correction_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  source_seq INTEGER NOT NULL,
  actor TEXT NOT NULL,
  reason TEXT NOT NULL,
  original_text TEXT NOT NULL,
  corrected_text TEXT NOT NULL,
  base_revision INTEGER NOT NULL,
  supersedes TEXT,
  lease_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(session_id, correction_id),
  FOREIGN KEY(session_id, supersedes) REFERENCES corrections(session_id, correction_id)
);

CREATE INDEX IF NOT EXISTS idx_corrections_segment
  ON corrections(session_id, source_id, source_seq, created_at);

CREATE INDEX IF NOT EXISTS idx_corrections_supersedes
  ON corrections(session_id, supersedes);

CREATE TABLE IF NOT EXISTS archive_extras (
  session_id TEXT NOT NULL,
  record_table TEXT NOT NULL,
  record_key TEXT NOT NULL,
  extras TEXT NOT NULL,
  PRIMARY KEY(session_id, record_table, record_key)
);

CREATE TABLE IF NOT EXISTS archive_unknown_records (
  session_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  record_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  PRIMARY KEY(session_id, seq)
);
`;

function hasColumn(
  db: Database.Database,
  table: string,
  column: string
): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{
    name: string;
  }>;
  return rows.some((row) => row.name === column);
}

export function initializeSchema(db: Database.Database): void {
  const current = db.pragma("user_version", { simple: true }) as number;
  if (current >= SCHEMA_VERSION) {
    return;
  }

  db.transaction(() => {
    db.exec(SCHEMA_SQL);

    if (current < 2) {
      if (!hasColumn(db, "revisions", "change_type")) {
        db.exec(
          `ALTER TABLE revisions ADD COLUMN change_type TEXT NOT NULL DEFAULT 'event' CHECK(change_type IN ('event', 'correction'))`
        );
      }
      if (!hasColumn(db, "revisions", "correction_id")) {
        db.exec(`ALTER TABLE revisions ADD COLUMN correction_id TEXT`);
      }
      db.exec(
        `CREATE INDEX IF NOT EXISTS idx_revisions_correction ON revisions(session_id, correction_id)`
      );
    }
  }).immediate();

  db.pragma(`user_version = ${SCHEMA_VERSION}`);
}
