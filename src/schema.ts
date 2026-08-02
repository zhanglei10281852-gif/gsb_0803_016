export const SCHEMA = `
CREATE TABLE IF NOT EXISTS sessions (
  session_id    TEXT PRIMARY KEY,
  last_revision INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
  session_id   TEXT NOT NULL,
  source_id    TEXT NOT NULL,
  event_id     TEXT NOT NULL,
  source_seq   INTEGER NOT NULL,
  kind         TEXT NOT NULL CHECK (kind IN ('partial','final')),
  text         TEXT NOT NULL,
  start_ms     INTEGER,
  content_hash TEXT NOT NULL,
  PRIMARY KEY (session_id, source_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_events_source_seq
  ON events (session_id, source_id, source_seq);

CREATE TABLE IF NOT EXISTS revisions (
  session_id  TEXT NOT NULL,
  revision    INTEGER NOT NULL,
  change_json TEXT NOT NULL,
  created_at  INTEGER NOT NULL,
  PRIMARY KEY (session_id, revision)
);

CREATE TABLE IF NOT EXISTS cursors (
  consumer_id    TEXT NOT NULL,
  session_id     TEXT NOT NULL,
  acked_revision INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (consumer_id, session_id)
);

-- Human review: one active lease per target segment.
CREATE TABLE IF NOT EXISTS leases (
  session_id       TEXT NOT NULL,
  target_source_id TEXT NOT NULL,
  target_event_id  TEXT NOT NULL,
  lease_id         TEXT NOT NULL,
  actor            TEXT NOT NULL,
  base_revision    INTEGER NOT NULL,
  expires_at       INTEGER NOT NULL,
  created_at       INTEGER NOT NULL,
  PRIMARY KEY (session_id, target_source_id, target_event_id)
);

-- Corrections never rewrite recognition events; they chain via supersedes.
CREATE TABLE IF NOT EXISTS corrections (
  session_id       TEXT NOT NULL,
  correction_id    TEXT NOT NULL,
  target_source_id TEXT NOT NULL,
  target_event_id  TEXT NOT NULL,
  text             TEXT NOT NULL,
  actor            TEXT NOT NULL,
  reason           TEXT NOT NULL,
  supersedes       TEXT,
  revision         INTEGER NOT NULL,
  created_at       INTEGER NOT NULL,
  PRIMARY KEY (session_id, correction_id)
);
CREATE INDEX IF NOT EXISTS idx_corrections_target
  ON corrections (session_id, target_source_id, target_event_id, revision);
`;

/**
 * v0.1 databases lack events.applied_revision; add it in place. It records
 * the stream revision that applied the event (used for stale-base checks).
 */
export const MIGRATIONS = [
  `ALTER TABLE events ADD COLUMN applied_revision INTEGER`,
];
