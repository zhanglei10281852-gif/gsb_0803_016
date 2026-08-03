import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { join } from "node:path";
import { RevisionHub } from "../../src/index";
import { tempDbPath } from "../helpers";

const WORKER = join(process.cwd(), "build", "test", "e2e", "correction-crash-worker.js");

/** Assert the store is internally consistent w.r.t. corrections + stream. */
function assertConsistent(path: string, sessionId: string): { corrected: number; head: number } {
  const hub = RevisionHub.open({ path });
  try {
    const head = hub.headRevision(sessionId);
    const revs = hub.database
      .prepare("SELECT revision FROM revisions WHERE session_id=? ORDER BY revision")
      .all(sessionId) as Array<{ revision: number }>;
    assert.equal(revs.length, head, "revision log dense: count equals head");
    for (let i = 0; i < revs.length; i++) assert.equal(revs[i]!.revision, i + 1, "no gaps");

    // Every corrected segment must have a matching correction lineage row and a
    // matching correction revision in the stream (no half-written correction).
    const corrected = hub.getSnapshot(sessionId).segments.filter((s) => s.origin === "correction");
    for (const seg of corrected) {
      const lineage = hub.database
        .prepare(
          `SELECT revision FROM corrections
            WHERE session_id=? AND segment_id=? AND revision=?`,
        )
        .get(sessionId, seg.segmentId, seg.revision);
      assert.ok(lineage, `segment ${seg.segmentId} flipped to correction without lineage`);
      const streamRow = hub.database
        .prepare(
          `SELECT origin, actor FROM revisions WHERE session_id=? AND revision=?`,
        )
        .get(sessionId, seg.revision) as { origin: string; actor: string | null };
      assert.equal(streamRow.origin, "correction");
      assert.equal(streamRow.actor, seg.actor);
    }
    return { corrected: corrected.length, head };
  } finally {
    hub.close();
  }
}

test("power loss during corrections leaves a consistent store and resumes", async () => {
  const sessionId = "ccrash";
  const segCount = 12;
  const { path, cleanup } = tempDbPath();
  try {
    // Seed recognised, finalised segments.
    const seed = RevisionHub.open({ path });
    for (let g = 0; g < segCount; g++) {
      seed.apply({ sessionId, sourceId: "src-0", segmentId: `seg-${g}`, eventId: `f-${g}`, sourceSeq: 1, kind: "final", text: `raw ${g}` });
    }
    seed.close();

    // Start correcting slowly, SIGKILL after a few commits.
    const doneAt = await new Promise<number>((resolve, reject) => {
      const child = spawn(
        process.execPath,
        [WORKER, path, sessionId, String(segCount), "reviewer-x", "8"],
        { stdio: ["ignore", "pipe", "inherit"] },
      );
      let last = 0;
      let buf = "";
      const killTarget = 5;
      child.stdout.on("data", (d: Buffer) => {
        buf += d.toString();
        let idx;
        while ((idx = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, idx).trim();
          buf = buf.slice(idx + 1);
          const m = /^done (\d+)$/.exec(line);
          if (m) {
            last = Number(m[1]);
            if (last >= killTarget) child.kill("SIGKILL");
          }
        }
      });
      child.on("exit", () => resolve(last));
      child.on("error", reject);
    });
    assert.ok(doneAt >= 1, "at least one correction should have committed");

    // After the crash: internally consistent, and progress survived.
    const afterCrash = assertConsistent(path, sessionId);
    assert.ok(afterCrash.corrected >= 1, "some corrections must have survived the crash");

    // Resume: finish correcting the remaining segments. Idempotent correctionIds
    // mean re-attempting an already-corrected segment is a stale-base no-op that
    // we simply skip; here we just correct whatever is still recognition-origin.
    const hub = RevisionHub.open({ path });
    try {
      for (const seg of hub.getSnapshot(sessionId).segments) {
        if (seg.origin === "correction") continue;
        const lease = hub.acquireLease({
          sessionId,
          sourceId: "src-0",
          segmentId: seg.segmentId,
          actor: "reviewer-resume",
          baseRevision: seg.revision,
          ttlMs: 60000,
        });
        hub.submitCorrection({
          leaseId: lease.leaseId,
          actor: "reviewer-resume",
          text: `resumed ${seg.segmentId}`,
          reason: "resume",
        });
      }
      const snap = hub.getSnapshot(sessionId);
      assert.equal(
        snap.segments.filter((s) => s.origin === "correction").length,
        segCount,
        "all segments corrected after resume",
      );
    } finally {
      hub.close();
    }

    // Final consistency check.
    const final = assertConsistent(path, sessionId);
    assert.equal(final.corrected, segCount);
  } finally {
    cleanup();
  }
});
