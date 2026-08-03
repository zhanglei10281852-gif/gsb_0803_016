import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { RevisionHub, type Snapshot } from "../../src/index";
import { tempDbPath } from "../helpers";

const IMPORT_WORKER = join(process.cwd(), "build", "test", "e2e", "archive-import-worker.js");

/** Build a non-trivial session (recognition + corrections + consumer cursor). */
function buildSession(hub: RevisionHub, sessionId: string, segCount: number): void {
  for (let g = 0; g < segCount; g++) {
    hub.apply({ sessionId, sourceId: "a", segmentId: `g${g}`, eventId: `a-p-${g}`, sourceSeq: g * 2 + 1, kind: "partial", text: `raw ${g}`, startMs: g * 100, endMs: g * 100 + 50 });
    hub.apply({ sessionId, sourceId: "a", segmentId: `g${g}`, eventId: `a-f-${g}`, sourceSeq: g * 2 + 2, kind: "final", text: `raw ${g}`, startMs: g * 100, endMs: g * 100 + 90 });
  }
  // Correct a couple of segments (lineage: actor/reason/supersedes).
  for (let g = 0; g < Math.min(3, segCount); g++) {
    const seg = hub.getSnapshot(sessionId).segments.find((s) => s.segmentId === `g${g}`)!;
    const lease = hub.acquireLease({ sessionId, sourceId: "a", segmentId: `g${g}`, actor: `rev-${g}`, baseRevision: seg.revision, ttlMs: 600000 });
    hub.submitCorrection({ leaseId: lease.leaseId, actor: `rev-${g}`, text: `fixed ${g}`, reason: `reason-${g}`, correctionId: `c-${g}` });
  }
  const batch = hub.pull(sessionId, "qc", { limit: 4 });
  hub.ack(sessionId, "qc", batch[batch.length - 1]!.revision);
}

function fingerprint(hub: RevisionHub, sessionId: string): string {
  const snap: Snapshot = hub.getSnapshot(sessionId);
  const revs = hub.pull(sessionId, "fp", { afterRevision: 0, limit: 100000 });
  return JSON.stringify({ head: snap.headRevision, segments: snap.segments, summary: snap.summary, revs });
}

test("fault injection: import killed mid-flight leaves no session, re-import succeeds", async () => {
  const sessionId = "isolated";
  const segCount = 40;

  // Source session + its archive on disk.
  const srcT = tempDbPath();
  const srcHub = RevisionHub.open({ path: srcT.path });
  buildSession(srcHub, sessionId, segCount);
  const srcFp = fingerprint(srcHub, sessionId);
  const srcCursor = srcHub.getCursor(sessionId, "qc");
  const archive = srcHub.exportSessionToString(sessionId);
  srcHub.close();

  const arcFile = join(process.cwd(), "build", "test", "e2e", "__isolated_archive.ndjson");
  writeFileSync(arcFile, archive, "utf8");

  const target = tempDbPath();
  try {
    // 1) Start an import worker and SIGKILL it before it can commit.
    await new Promise<void>((resolve, reject) => {
      const child = spawn(
        process.execPath,
        [IMPORT_WORKER, target.path, arcFile, sessionId, "4000"], // linger 4s pre-import
        { stdio: ["ignore", "pipe", "inherit"] },
      );
      let buf = "";
      child.stdout.on("data", (d: Buffer) => {
        buf += d.toString();
        if (buf.includes("start")) {
          // Kill during the pre-commit window.
          child.kill("SIGKILL");
        }
        if (buf.includes("committed")) {
          reject(new Error("worker committed before we could kill it (increase holdMs)"));
        }
      });
      child.on("exit", () => resolve());
      child.on("error", reject);
    });

    // 2) The killed import must have left NOTHING visible in the target.
    {
      const hub = RevisionHub.open({ path: target.path });
      try {
        assert.equal(hub.headRevision(sessionId), 0, "no head after killed import");
        assert.equal(hub.getSnapshot(sessionId).segments.length, 0, "no segments after killed import");
        assert.equal(hub.pull(sessionId, "probe", { afterRevision: 0 }).length, 0, "no revisions");
      } finally {
        hub.close();
      }
    }

    // 3) Re-import cleanly and verify equivalence to the source.
    {
      const hub = RevisionHub.open({ path: target.path });
      try {
        const res = hub.importSession(archive);
        assert.equal(res.imported, true, "clean re-import after crash");
        assert.equal(fingerprint(hub, sessionId), srcFp, "imported state equals source");
        assert.equal(hub.getCursor(sessionId, "qc"), srcCursor, "cursor carried");

        // 4) Idempotency holds even after the earlier crash: a second import no-ops.
        const again = hub.importSession(archive);
        assert.equal(again.imported, false, "second import idempotent");
      } finally {
        hub.close();
      }
    }
  } finally {
    target.cleanup();
    srcT.cleanup();
  }
});

test("fault injection: concurrent double import is idempotent (one winner, one no-op)", async () => {
  const sessionId = "race";
  const segCount = 30;

  const srcT = tempDbPath();
  const srcHub = RevisionHub.open({ path: srcT.path });
  buildSession(srcHub, sessionId, segCount);
  const srcFp = fingerprint(srcHub, sessionId);
  const archive = srcHub.exportSessionToString(sessionId);
  srcHub.close();

  const arcFile = join(process.cwd(), "build", "test", "e2e", "__race_archive.ndjson");
  writeFileSync(arcFile, archive, "utf8");

  const target = tempDbPath();
  try {
    // Two importer processes race to import the same archive into the same DB.
    const run = () =>
      new Promise<string>((resolve, reject) => {
        const child = spawn(
          process.execPath,
          [IMPORT_WORKER, target.path, arcFile, sessionId, "0"],
          { stdio: ["ignore", "pipe", "inherit"] },
        );
        let buf = "";
        child.stdout.on("data", (d: Buffer) => (buf += d.toString()));
        child.on("exit", (code) => (code === 0 ? resolve(buf) : reject(new Error(`exit ${code}: ${buf}`))));
        child.on("error", reject);
      });

    const [o1, o2] = await Promise.all([run(), run()]);
    const importedTrue = [o1, o2].filter((o) => /committed true/.test(o)).length;
    const importedFalse = [o1, o2].filter((o) => /committed false/.test(o)).length;
    // Both may print "true" only if serialized such that... no: the ledger makes
    // exactly one a real import and the other a no-op. Under IMMEDIATE locking,
    // one commits first; the other sees the ledger and no-ops.
    assert.equal(importedTrue, 1, "exactly one import should write");
    assert.equal(importedFalse, 1, "the other should be an idempotent no-op");

    const hub = RevisionHub.open({ path: target.path });
    try {
      assert.equal(fingerprint(hub, sessionId), srcFp, "final state equals source once");
      // No duplication: revision log dense 1..head.
      const revs = hub.database
        .prepare("SELECT revision FROM revisions WHERE session_id=? ORDER BY revision")
        .all(sessionId) as Array<{ revision: number }>;
      assert.equal(revs.length, hub.headRevision(sessionId));
      for (let i = 0; i < revs.length; i++) assert.equal(revs[i]!.revision, i + 1);
    } finally {
      hub.close();
    }
  } finally {
    target.cleanup();
    srcT.cleanup();
  }
});
