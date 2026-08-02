import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { join } from "node:path";
import { RevisionHub } from "../../src/index";
import { tempDbPath } from "../helpers";
import { makeEvents, fingerprint } from "./shared";

const CRASH_WORKER = join(process.cwd(), "build", "test", "e2e", "crash-worker.js");

interface DbCheck {
  head: number;
  revCount: number;
  dense: boolean;
  maxSegRev: number;
}

/** Open the DB read-only-ish and assert internal invariants after a crash. */
function inspect(path: string, sessionId: string): DbCheck {
  const hub = RevisionHub.open({ path });
  try {
    const head = hub.headRevision(sessionId);
    const revs = hub.database
      .prepare("SELECT revision FROM revisions WHERE session_id=? ORDER BY revision")
      .all(sessionId) as Array<{ revision: number }>;
    let dense = revs.length === head;
    for (let i = 0; i < revs.length; i++) if (revs[i]!.revision !== i + 1) dense = false;
    const maxSegRow = hub.database
      .prepare("SELECT COALESCE(MAX(revision),0) AS m FROM segments WHERE session_id=?")
      .get(sessionId) as { m: number };
    return { head, revCount: revs.length, dense, maxSegRev: maxSegRow.m };
  } finally {
    hub.close();
  }
}

test("hard kill (power loss) leaves a consistent store and resumes correctly", async () => {
  const sessionId = "crash";
  const sources = 3;
  const segs = 6;
  const partials = 4;

  const { path, cleanup } = tempDbPath();
  try {
    // 1) Start a worker that writes slowly, then SIGKILL it partway through.
    const killedAt = await new Promise<number>((resolve, reject) => {
      const child = spawn(
        process.execPath,
        [CRASH_WORKER, path, sessionId, String(sources), String(segs), String(partials), "6"],
        { stdio: ["ignore", "pipe", "inherit"] },
      );
      let lastApplied = 0;
      let buf = "";
      const killTarget = 20; // kill after ~20 committed events
      child.stdout.on("data", (d: Buffer) => {
        buf += d.toString();
        let idx;
        while ((idx = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, idx).trim();
          buf = buf.slice(idx + 1);
          const m = /^applied (\d+)$/.exec(line);
          if (m) {
            lastApplied = Number(m[1]);
            if (lastApplied >= killTarget) child.kill("SIGKILL");
          }
        }
      });
      child.on("exit", () => resolve(lastApplied));
      child.on("error", reject);
    });

    assert.ok(killedAt >= 1, "worker should have applied at least one event before dying");

    // 2) After the crash, the store must be internally consistent: the revision
    //    log is dense 1..head and no segment references a revision beyond head.
    const afterCrash = inspect(path, sessionId);
    assert.ok(afterCrash.dense, "revision log must be dense after crash (no half-writes)");
    assert.ok(
      afterCrash.maxSegRev <= afterCrash.head,
      "no segment may reference a revision beyond head",
    );
    assert.ok(afterCrash.head >= 1, "some progress must have survived");

    // 3) Reopen and replay the full stream. Re-delivered events are idempotent,
    //    so the session must converge to the same state as a clean run.
    const hub = RevisionHub.open({ path });
    try {
      for (const ev of makeEvents(sessionId, sources, segs, partials)) hub.apply(ev);
      const snap = hub.getSnapshot(sessionId);
      assert.equal(snap.segments.length, sources * segs);
      assert.equal(snap.segments.filter((s) => s.kind === "final").length, sources * segs);
    } finally {
      hub.close();
    }

    // 4) Compare against a pristine baseline computed elsewhere.
    const b = tempDbPath();
    const baseHub = RevisionHub.open({ path: b.path });
    let baseFp: string;
    try {
      for (const ev of makeEvents(sessionId, sources, segs, partials)) baseHub.apply(ev);
      const bs = baseHub.getSnapshot(sessionId);
      baseFp = fingerprint(bs.segments as unknown as Array<Record<string, unknown>>, bs.summary);
    } finally {
      baseHub.close();
      b.cleanup();
    }

    const resumed = RevisionHub.open({ path });
    try {
      const rs = resumed.getSnapshot(sessionId);
      const fp = fingerprint(rs.segments as unknown as Array<Record<string, unknown>>, rs.summary);
      assert.equal(fp, baseFp, "resumed state must match pristine baseline");
    } finally {
      resumed.close();
    }
  } finally {
    cleanup();
  }
});
