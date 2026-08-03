import Database from "better-sqlite3";
import type { RevisionHubOptions } from "./types";

export type DB = Database.Database;

/**
 * The schema is deliberately small and normalised around these concerns:
 *
 *  - `events`      : the append-only log of everything we were told, keyed by
 *                    the source-stable (session, source, eventId). This is the
 *                    ground truth used for idempotency and conflict detection.
 *                    Human corrections NEVER write here — raw recognition is
 *                    preserved verbatim.
 *  - `segments`    : the resolved winner per (session, source, segmentId),
 *                    including its provenance (recognition vs correction).
 *  - `revisions`   : the durable, session-monotonic change stream. Recognition
 *                    and correction revisions share this one stream so existing
 *                    consumers read corrections as ordinary new revisions.
 *  - `corrections` : append-only lineage of human corrections (actor, reason,
 *                    supersedes), separate from raw events.
 *  - `leases`      : time-boxed, single-winner correction leases per segment.
 *  - `sessions`    : per-session head revision counter (revision allocator).
 *  - `consumers`   : durable per-consumer read cursors.
 *  - `imported_archives` : idempotency ledger of imported archive digests.
 *  - `archive_ext` : forward-compat store of unknown optional fields seen in an
 *                    imported archive, re-emitted verbatim on re-export.
 *
 * Every write path touches these tables inside a single IMMEDIATE transaction,
 * so a crash can never leave a revision without its segment update, or a
 * cursor advanced past data that was rolled back.
 */
const SCHEMA_VERSION = 4;

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  session_id    TEXT PRIMARY KEY,
  head_revision INTEGER NOT NULL DEFAULT 0,
  compacted_upto INTEGER NOT NULL DEFAULT 0
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
  origin      TEXT NOT NULL DEFAULT 'recognition',
  actor       TEXT,
  PRIMARY KEY (session_id, source_id, segment_id)
);

-- Durable, per-session monotonic change stream (recognition + corrections).
CREATE TABLE IF NOT EXISTS revisions (
  session_id          TEXT NOT NULL,
  revision            INTEGER NOT NULL,
  source_id           TEXT NOT NULL,
  segment_id          TEXT NOT NULL,
  kind                TEXT NOT NULL,
  text                TEXT NOT NULL,
  start_ms            INTEGER,
  end_ms              INTEGER,
  event_id            TEXT NOT NULL,
  source_seq          INTEGER NOT NULL,
  created_at          INTEGER NOT NULL,
  origin              TEXT NOT NULL DEFAULT 'recognition',
  actor               TEXT,
  reason              TEXT,
  correction_id       TEXT,
  supersedes_revision INTEGER,
  PRIMARY KEY (session_id, revision)
);
CREATE INDEX IF NOT EXISTS idx_revisions_session ON revisions (session_id, revision);

-- Append-only lineage of human corrections (never touches raw events).
CREATE TABLE IF NOT EXISTS corrections (
  session_id          TEXT NOT NULL,
  correction_id       TEXT NOT NULL,
  source_id           TEXT NOT NULL,
  segment_id          TEXT NOT NULL,
  actor               TEXT NOT NULL,
  reason              TEXT NOT NULL,
  text                TEXT NOT NULL,
  base_revision       INTEGER NOT NULL,
  revision            INTEGER NOT NULL,
  supersedes_revision INTEGER NOT NULL,
  lease_id            TEXT NOT NULL,
  created_at          INTEGER NOT NULL,
  PRIMARY KEY (session_id, correction_id)
);

-- Time-boxed, single-winner correction leases per segment.
CREATE TABLE IF NOT EXISTS leases (
  session_id    TEXT NOT NULL,
  source_id     TEXT NOT NULL,
  segment_id    TEXT NOT NULL,
  lease_id      TEXT NOT NULL,
  actor         TEXT NOT NULL,
  base_revision INTEGER NOT NULL,
  acquired_at   INTEGER NOT NULL,
  expires_at    INTEGER NOT NULL,
  released      INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (session_id, source_id, segment_id)
);
CREATE INDEX IF NOT EXISTS idx_leases_id ON leases (session_id, lease_id);

-- Durable read cursors.
CREATE TABLE IF NOT EXISTS consumers (
  session_id       TEXT NOT NULL,
  consumer_id      TEXT NOT NULL,
  cursor           INTEGER NOT NULL DEFAULT 0,
  updated_at       INTEGER NOT NULL,
  lease_expires_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (session_id, consumer_id)
);

-- Verifiable compaction checkpoints: a self-contained archive of the session
-- as of the checkpoint revision, plus its integrity digest, so a reset consumer
-- can be rebuilt to an exact snapshot and compaction can prove coverage.
CREATE TABLE IF NOT EXISTS checkpoints (
  session_id  TEXT NOT NULL,
  revision    INTEGER NOT NULL,
  digest      TEXT NOT NULL,
  keyed       INTEGER NOT NULL DEFAULT 0,
  archive     TEXT NOT NULL,
  created_at  INTEGER NOT NULL,
  PRIMARY KEY (session_id, revision)
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_session ON checkpoints (session_id, revision);

-- Idempotency ledger: which archive (by content digest) populated a session.
CREATE TABLE IF NOT EXISTS imported_archives (
  session_id  TEXT PRIMARY KEY,
  digest      TEXT NOT NULL,
  format      INTEGER NOT NULL,
  imported_at INTEGER NOT NULL
);

-- Forward-compat: unknown optional fields carried by imported archive records,
-- keyed by record identity so they can be re-emitted verbatim on export.
CREATE TABLE IF NOT EXISTS archive_ext (
  session_id  TEXT NOT NULL,
  record_type TEXT NOT NULL,
  record_key  TEXT NOT NULL,
  ext_json    TEXT NOT NULL,
  PRIMARY KEY (session_id, record_type, record_key)
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

  // Create any missing tables first (fresh DBs get the full v2 shape).
  db.exec(SCHEMA_SQL);

  const row = db.prepare("SELECT value FROM meta WHERE key = 'schema_version'").get() as
    | { value: string }
    | undefined;
  const current = row ? Number(row.value) : 0;

  if (current < SCHEMA_VERSION) {
    migrate(db, current);
    db.prepare(
      `INSERT INTO meta (key, value) VALUES ('schema_version', ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
    ).run(String(SCHEMA_VERSION));
  }
  return db;
}

/** Column names that already exist on a table. */
function columnsOf(db: DB, table: string): Set<string> {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  return new Set(rows.map((r) => r.name));
}

/**
 * Forward-only migrations. v1 stores predate the correction/lease feature, so
 * we add the provenance columns to `segments`/`revisions` in place (existing
 * rows default to origin='recognition'). v2→v3 only adds new tables
 * (`imported_archives`, `archive_ext`), which SCHEMA_SQL already created above,
 * so there is nothing extra to do for that step.
 */
function migrate(db: DB, from: number): void {
  const run = db.transaction(() => {
    if (from < 2) {
      const segCols = columnsOf(db, "segments");
      if (!segCols.has("origin"))
        db.exec("ALTER TABLE segments ADD COLUMN origin TEXT NOT NULL DEFAULT 'recognition'");
      if (!segCols.has("actor")) db.exec("ALTER TABLE segments ADD COLUMN actor TEXT");

      const revCols = columnsOf(db, "revisions");
      if (!revCols.has("origin"))
        db.exec("ALTER TABLE revisions ADD COLUMN origin TEXT NOT NULL DEFAULT 'recognition'");
      if (!revCols.has("actor")) db.exec("ALTER TABLE revisions ADD COLUMN actor TEXT");
      if (!revCols.has("reason")) db.exec("ALTER TABLE revisions ADD COLUMN reason TEXT");
      if (!revCols.has("correction_id"))
        db.exec("ALTER TABLE revisions ADD COLUMN correction_id TEXT");
      if (!revCols.has("supersedes_revision"))
        db.exec("ALTER TABLE revisions ADD COLUMN supersedes_revision INTEGER");
    }
    // from < 3: tables added by SCHEMA_SQL; no column migration required.
    if (from < 4) {
      // v3→v4 adds compaction: watermark on sessions, lease on consumers, and
      // the `checkpoints` table (created by SCHEMA_SQL). Existing rows default
      // to compacted_upto=0 / lease_expires_at=0 (no lease, nothing recycled).
      const sessCols = columnsOf(db, "sessions");
      if (!sessCols.has("compacted_upto"))
        db.exec("ALTER TABLE sessions ADD COLUMN compacted_upto INTEGER NOT NULL DEFAULT 0");
      const conCols = columnsOf(db, "consumers");
      if (!conCols.has("lease_expires_at"))
        db.exec("ALTER TABLE consumers ADD COLUMN lease_expires_at INTEGER NOT NULL DEFAULT 0");
    }
  });
  run();
}
