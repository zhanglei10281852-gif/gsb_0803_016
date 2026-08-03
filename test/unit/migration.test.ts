import { test } from "node:test";
import assert from "node:assert/strict";
import Database from "better-sqlite3";
import { RevisionHub } from "../../src/index";
import { tempDbPath } from "../helpers";

/**
 * Build a v1-shaped store by hand (the schema shipped before the correction/
 * lease feature) with one recognised segment and one revision, then open it
 * with the current hub and assert the migration adds provenance columns
 * (defaulting to "recognition") and that the correction flow works afterwards.
 */
function makeV1Db(path: string): void {
  const db = new Database(path);
  db.pragma("journal_mode = WAL");
  db.exec(`
    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE sessions (session_id TEXT PRIMARY KEY, head_revision INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE events (
      session_id TEXT NOT NULL, source_id TEXT NOT NULL, event_id TEXT NOT NULL,
      segment_id TEXT NOT NULL, source_seq INTEGER NOT NULL, kind TEXT NOT NULL,
      text TEXT NOT NULL, start_ms INTEGER, end_ms INTEGER, content_hash TEXT NOT NULL,
      received_at INTEGER NOT NULL, PRIMARY KEY (session_id, source_id, event_id));
    CREATE TABLE segments (
      session_id TEXT NOT NULL, source_id TEXT NOT NULL, segment_id TEXT NOT NULL,
      kind TEXT NOT NULL, text TEXT NOT NULL, start_ms INTEGER, end_ms INTEGER,
      source_seq INTEGER NOT NULL, event_id TEXT NOT NULL, finalized INTEGER NOT NULL DEFAULT 0,
      revision INTEGER NOT NULL, PRIMARY KEY (session_id, source_id, segment_id));
    CREATE TABLE revisions (
      session_id TEXT NOT NULL, revision INTEGER NOT NULL, source_id TEXT NOT NULL,
      segment_id TEXT NOT NULL, kind TEXT NOT NULL, text TEXT NOT NULL, start_ms INTEGER,
      end_ms INTEGER, event_id TEXT NOT NULL, source_seq INTEGER NOT NULL,
      created_at INTEGER NOT NULL, PRIMARY KEY (session_id, revision));
    CREATE TABLE consumers (
      session_id TEXT NOT NULL, consumer_id TEXT NOT NULL, cursor INTEGER NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL, PRIMARY KEY (session_id, consumer_id));
  `);
  db.prepare("INSERT INTO meta (key,value) VALUES ('schema_version','1')").run();
  db.prepare("INSERT INTO sessions (session_id, head_revision) VALUES ('s', 1)").run();
  db.prepare(
    `INSERT INTO events (session_id, source_id, event_id, segment_id, source_seq, kind, text, start_ms, end_ms, content_hash, received_at)
     VALUES ('s','a','e1','g1',1,'final','legacy',0,10,'h',123)`,
  ).run();
  db.prepare(
    `INSERT INTO segments (session_id, source_id, segment_id, kind, text, start_ms, end_ms, source_seq, event_id, finalized, revision)
     VALUES ('s','a','g1','final','legacy',0,10,1,'e1',1,1)`,
  ).run();
  db.prepare(
    `INSERT INTO revisions (session_id, revision, source_id, segment_id, kind, text, start_ms, end_ms, event_id, source_seq, created_at)
     VALUES ('s',1,'a','g1','final','legacy',0,10,'e1',1,123)`,
  ).run();
  db.close();
}

test("v1 store migrates to v2: existing rows default to recognition origin", () => {
  const { path, cleanup } = tempDbPath();
  try {
    makeV1Db(path);

    const hub = RevisionHub.open({ path });
    try {
      // Existing snapshot survives with recognition provenance.
      const snap = hub.getSnapshot("s");
      assert.equal(snap.segments.length, 1);
      const seg = snap.segments[0]!;
      assert.equal(seg.text, "legacy");
      assert.equal(seg.origin, "recognition");
      assert.equal(seg.actor, null);

      // Existing consumer replay still works and reports provenance.
      const revs = hub.pull("s", "legacy-consumer", { afterRevision: 0 });
      assert.equal(revs.length, 1);
      assert.equal(revs[0]!.origin, "recognition");
      assert.equal(revs[0]!.correctionId, null);

      // And the new correction flow works on the migrated store.
      const lease = hub.acquireLease({ sessionId: "s", sourceId: "a", segmentId: "g1", actor: "r", baseRevision: seg.revision, ttlMs: 60000 });
      const res = hub.submitCorrection({ leaseId: lease.leaseId, actor: "r", text: "fixed legacy", reason: "migration test" });
      assert.equal(res.applied, true);
      assert.equal(hub.getSnapshot("s").segments[0]!.origin, "correction");

      // Schema version is now current (>= 3).
      const v = hub.database.prepare("SELECT value FROM meta WHERE key='schema_version'").get() as { value: string };
      assert.ok(Number(v.value) >= 3, `schema version should be >= 3, got ${v.value}`);
    } finally {
      hub.close();
    }
  } finally {
    cleanup();
  }
});
