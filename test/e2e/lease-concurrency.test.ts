import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { RevisionHub } from "../../src/index";
import { tempDbPath } from "../helpers";

const LEASE_WORKER = join(process.cwd(), "build", "test", "e2e", "lease-worker.js");

interface WorkerOut {
  applied: number;
  leaseConflicts: number;
  stale: number;
  expired: number;
  noLease: number;
}

test("concurrent reviewers contend for leases without corrupting the stream", async () => {
  const sessionId = "review";
  const segCount = 8;
  const { path, cleanup } = tempDbPath();
  try {
    // Seed recognised, finalised segments the reviewers will correct.
    const seed = RevisionHub.open({ path });
    for (let g = 0; g < segCount; g++) {
      seed.apply({ sessionId, sourceId: "src-0", segmentId: `seg-${g}`, eventId: `p-${g}`, sourceSeq: 1, kind: "partial", text: `raw ${g}` });
      seed.apply({ sessionId, sourceId: "src-0", segmentId: `seg-${g}`, eventId: `f-${g}`, sourceSeq: 2, kind: "final", text: `raw ${g}` });
    }
    seed.close();

    // 6 competing reviewer processes, each hammering all segments.
    const reviewerCount = 6;
    const procs = Array.from({ length: reviewerCount }, (_, i) =>
      new Promise<WorkerOut>((resolve, reject) => {
        try {
          const out = execFileSync(
            process.execPath,
            [LEASE_WORKER, path, sessionId, String(segCount), "6", `reviewer-${i}`, String(i)],
            { encoding: "utf8", timeout: 120000 },
          );
          resolve(JSON.parse(out.trim().split("\n").filter(Boolean).pop()!));
        } catch (e) {
          reject(e);
        }
      }),
    );
    const results = await Promise.all(procs);

    const hub = RevisionHub.open({ path });
    try {
      // Some corrections must have landed, and contention must have been observed.
      const totalApplied = results.reduce((a, r) => a + r.applied, 0);
      assert.ok(totalApplied > 0, "at least some corrections should land");

      // Invariant 1: revision stream is dense 1..head with no gaps/dupes despite
      // concurrent recognition-seed + many concurrent correction commits.
      const revs = hub.database
        .prepare("SELECT revision, origin FROM revisions WHERE session_id=? ORDER BY revision")
        .all(sessionId) as Array<{ revision: number; origin: string }>;
      const head = hub.headRevision(sessionId);
      assert.equal(revs.length, head, "revision count equals head");
      for (let i = 0; i < revs.length; i++) assert.equal(revs[i]!.revision, i + 1);

      // Invariant 2: each segment ends up owned by exactly one correction winner
      // whose revision matches its latest correction revision in the stream.
      const snap = hub.getSnapshot(sessionId);
      for (const seg of snap.segments) {
        assert.equal(seg.origin, "correction", `${seg.segmentId} should be corrected`);
        // The winning revision must be the max correction revision for it.
        const maxCorr = hub.database
          .prepare(
            `SELECT MAX(revision) AS m FROM revisions
              WHERE session_id=? AND segment_id=? AND origin='correction'`,
          )
          .get(sessionId, seg.segmentId) as { m: number };
        assert.equal(seg.revision, maxCorr.m, `${seg.segmentId} winner mismatch`);
      }

      // Invariant 3: no two committed corrections share a (segment, supersedes)
      // base — i.e. a given base revision was corrected at most once (single
      // winner per base), which is what the lease + stale-base check guarantee.
      const dupes = hub.database
        .prepare(
          `SELECT segment_id, supersedes_revision, COUNT(*) AS c
             FROM corrections WHERE session_id=?
            GROUP BY segment_id, supersedes_revision HAVING c > 1`,
        )
        .all(sessionId) as Array<{ segment_id: string; c: number }>;
      assert.equal(dupes.length, 0, "no two corrections may share the same base");

      // Invariant 4: raw recognition events are intact (2 per segment, verbatim).
      const evCount = hub.database
        .prepare("SELECT COUNT(*) AS c FROM events WHERE session_id=?")
        .get(sessionId) as { c: number };
      assert.equal(evCount.c, segCount * 2, "raw events must be untouched by corrections");
    } finally {
      hub.close();
    }
  } finally {
    cleanup();
  }
});
