import type Database from "better-sqlite3";

export const SCHEMA_VERSION = 1;

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
  event_id TEXT,
  source_id TEXT,
  source_seq INTEGER,
  kind TEXT,
  snapshot TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(session_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_revisions_event
  ON revisions(session_id, event_id);

CREATE TABLE IF NOT EXISTS consumer_cursors (
  session_id TEXT NOT NULL,
  consumer_id TEXT NOT NULL,
  cursor INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(session_id, consumer_id)
);
`;

export function initializeSchema(db: Database.Database): void {
  const current = db.pragma("user_version", { simple: true }) as number;
  if (current >= SCHEMA_VERSION) {
    return;
  }

  db.transaction(() => {
    db.exec(SCHEMA_SQL);
  }).immediate();

  db.pragma(`user_version = ${SCHEMA_VERSION}`);
}
