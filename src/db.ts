import Database from "better-sqlite3";
import type { RevisionHubOptions } from "./types";

export type DB = Database.Database;

/**
 * The schema is deliberately small and normalised around three concerns:
 *
 *  - `events`      : the append-only log of everything we were told, keyed by
 *                    the source-stable (session, source, eventId). This is the
 *                    ground truth used for idempotency and conflict detection.
 *  - `segments`    : the resolved winner per (session, source, segmentId).
 *  - `revisions`   : the durable, session-monotonic change stream.
 *  - `sessions`    : per-session head revision counter (revision allocator).
 *  - `consumers`   : durable per-consumer read cursors.
 *
 * Every write path touches these tables inside a single IMMEDIATE transaction,
 * so a crash can never leave a revision without its segment update, or a
 * cursor advanced past data that was rolled back.
 */
const SCHEMA_VERSION = 1;

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  session_id    TEXT PRIMARY KEY,
  head_revision INTEGER NOT NULL DEFAULT 0
);

-- Append-only record of every distinct event we accepted or rejected-by-content.
CREATE TABLE IF NOT EXISTS events (
  session_id   TEXT NOT NULL,
  source_id    TEXT NOT NULL,
  event_id     TEXT NOT NULL,
  segment_id   TEXT NOT NULL,
  source_seq   INTEGER NOT NULL,
  kind         TEXT NOT NULL,
  text         TEXT NOT NULL,
  start_ms     INTEGER,
  end_ms       INTEGER,
  content_hash TEXT NOT NULL,
  received_at  INTEGER NOT NULL,
  PRIMARY KEY (session_id, source_id, event_id)
);

-- The current resolved state per segment.
CREATE TABLE IF NOT EXISTS segments (
  session_id  TEXT NOT NULL,
  source_id   TEXT NOT NULL,
  segment_id  TEXT NOT NULL,
  kind        TEXT NOT NULL,
  text        TEXT NOT NULL,
  start_ms    INTEGER,
  end_ms      INTEGER,
  source_seq  INTEGER NOT NULL,
  event_id    TEXT NOT NULL,
  finalized   INTEGER NOT NULL DEFAULT 0,
  revision    INTEGER NOT NULL,
  PRIMARY KEY (session_id, source_id, segment_id)
);

-- Durable, per-session monotonic change stream.
CREATE TABLE IF NOT EXISTS revisions (
  session_id  TEXT NOT NULL,
  revision    INTEGER NOT NULL,
  source_id   TEXT NOT NULL,
  segment_id  TEXT NOT NULL,
  kind        TEXT NOT NULL,
  text        TEXT NOT NULL,
  start_ms    INTEGER,
  end_ms      INTEGER,
  event_id    TEXT NOT NULL,
  source_seq  INTEGER NOT NULL,
  created_at  INTEGER NOT NULL,
  PRIMARY KEY (session_id, revision)
);
CREATE INDEX IF NOT EXISTS idx_revisions_session ON revisions (session_id, revision);

-- Durable read cursors.
CREATE TABLE IF NOT EXISTS consumers (
  session_id   TEXT NOT NULL,
  consumer_id  TEXT NOT NULL,
  cursor       INTEGER NOT NULL DEFAULT 0,
  updated_at   INTEGER NOT NULL,
  PRIMARY KEY (session_id, consumer_id)
);
`;

export function openDb(opts: RevisionHubOptions): DB {
  const db = new Database(opts.path);
  // Durability + cross-process concurrency configuration.
  // WAL lets a reader proceed while a writer holds the lock, and lets multiple
  // processes share the file. FULL fsync survives power loss; busy_timeout
  // makes concurrent writers wait for the lock instead of throwing SQLITE_BUSY.
  db.pragma("journal_mode = WAL");
  db.pragma(`synchronous = ${opts.synchronous ?? "FULL"}`);
  db.pragma("foreign_keys = ON");
  db.pragma(`busy_timeout = ${opts.busyTimeoutMs ?? 5000}`);

  db.exec(SCHEMA_SQL);
  const row = db.prepare("SELECT value FROM meta WHERE key = 'schema_version'").get() as
    | { value: string }
    | undefined;
  if (!row) {
    db.prepare("INSERT INTO meta (key, value) VALUES ('schema_version', ?)").run(
      String(SCHEMA_VERSION),
    );
  }
  return db;
}
