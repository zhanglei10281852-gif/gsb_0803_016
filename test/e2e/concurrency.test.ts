import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { RevisionHub } from "../../src/index";
import { tempDbPath } from "../helpers";
import { makeEvents, fingerprint } from "./shared";

const WORKER = join(process.cwd(), "build", "test", "e2e", "worker.js");

/** Compute the authoritative resolved state in a single clean process. */
function baseline(sessionId: string, sources: number, segs: number, partials: number) {
  const { path, cleanup } = tempDbPath();
  const hub = RevisionHub.open({ path });
  try {
    for (const ev of makeEvents(sessionId, sources, segs, partials)) hub.apply(ev);
    const snap = hub.getSnapshot(sessionId);
    return {
      fp: fingerprint(snap.segments as unknown as Array<Record<string, unknown>>, snap.summary),
      segCount: snap.segments.length,
    };
  } finally {
    hub.close();
    cleanup();
  }
}

test("many concurrent OS processes converge to the identical snapshot", async () => {
  const sessionId = "concurrent";
  const sources = 4;
  const segs = 6;
  const partials = 4;
  const base = baseline(sessionId, sources, segs, partials);

  const { path, cleanup } = tempDbPath();
  const workerCount = 5;
  try {
    // Spawn workers in parallel; each writes to the same DB file concurrently.
    const procs = Array.from({ length: workerCount }, (_, i) =>
      new Promise<{ applied: number; effective: number; conflicts: number }>((resolve, reject) => {
        try {
          const out = execFileSync(
            process.execPath,
            [
              WORKER,
              path,
              sessionId,
              String(sources),
              String(segs),
              String(partials),
              String(i),
              String(workerCount),
              String(1234 + i),
              "0.25", // 25% overlap → same events delivered by multiple processes
            ],
            { encoding: "utf8", timeout: 120000 },
          );
          const line = out.trim().split("\n").filter(Boolean).pop()!;
          resolve(JSON.parse(line));
        } catch (e) {
          reject(e);
        }
      }),
    );
    const results = await Promise.all(procs);

    // Verify the resolved state matches the single-process baseline exactly.
    const hub = RevisionHub.open({ path });
    try {
      const snap = hub.getSnapshot(sessionId);
      const fp = fingerprint(
        snap.segments as unknown as Array<Record<string, unknown>>,
        snap.summary,
      );
      assert.equal(snap.segments.length, base.segCount, "segment count mismatch");
      assert.equal(fp, base.fp, "concurrent snapshot diverged from baseline");

      // Revision stream must be dense 1..head with no gaps or duplicates.
      const revs = hub.database
        .prepare("SELECT revision FROM revisions WHERE session_id=? ORDER BY revision")
        .all(sessionId) as Array<{ revision: number }>;
      const head = hub.headRevision(sessionId);
      assert.equal(revs.length, head, "revision count must equal head revision");
      for (let i = 0; i < revs.length; i++) {
        assert.equal(revs[i]!.revision, i + 1, `revision gap at index ${i}`);
      }

      // Every final segment must actually be final (no partial rollback survived).
      const finals = snap.segments.filter((s) => s.kind === "final").length;
      assert.equal(finals, sources * segs, "all segments should resolve to final");

      const totalEffective = results.reduce((a, r) => a + r.effective, 0);
      assert.equal(totalEffective, head, "sum of effective applies must equal head revision");
    } finally {
      hub.close();
    }
  } finally {
    cleanup();
  }
});
