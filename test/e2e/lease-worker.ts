/**
 * Lease-contention worker. Many of these run at once against the SAME SQLite
 * file, each acting as a reviewer that repeatedly tries to acquire a lease on
 * one of a fixed set of segments, and — if it wins — submits a correction.
 *
 * This exercises single-winner lease acquisition and correction commits under
 * real cross-process contention. Each worker reports how many corrections it
 * actually landed, plus how many lease conflicts / stale-base / expired
 * rejections it saw (all expected and benign under contention).
 *
 * Args: <dbPath> <sessionId> <segments> <attemptsPerSegment> <actor> <seed>
 * Emits one JSON line: {"applied":n,"leaseConflicts":n,"stale":n,"expired":n,"noLease":n}
 */
import {
  RevisionHub,
  LeaseConflictError,
  StaleBaseError,
  LeaseExpiredError,
  NoLeaseError,
} from "../../src/index";

function main(): void {
  const [dbPath, sessionId, segCount, attempts, actor] = process.argv.slice(2);
  const hub = RevisionHub.open({ path: dbPath!, busyTimeoutMs: 20000 });

  let applied = 0;
  let leaseConflicts = 0;
  let stale = 0;
  let expired = 0;
  let noLease = 0;

  try {
    for (let a = 0; a < Number(attempts); a++) {
      for (let g = 0; g < Number(segCount); g++) {
        const sourceId = "src-0";
        const segmentId = `seg-${g}`;
        // Base our correction on whatever the segment currently shows.
        const snap = hub.getSnapshot(sessionId!);
        const seg = snap.segments.find((s) => s.segmentId === segmentId);
        if (!seg) continue; // recognition may not have produced it yet
        try {
          const lease = hub.acquireLease({
            sessionId: sessionId!,
            sourceId,
            segmentId,
            actor: actor!,
            baseRevision: seg.revision,
            ttlMs: 5000,
          });
          hub.submitCorrection({
            leaseId: lease.leaseId,
            actor: actor!,
            text: `${actor} fixed ${segmentId} @${seg.revision}`,
            reason: "review",
            // Deterministic id so accidental double-submit is idempotent.
            correctionId: `${actor}:${segmentId}:${seg.revision}`,
          });
          applied += 1;
        } catch (err) {
          if (err instanceof LeaseConflictError) leaseConflicts += 1;
          else if (err instanceof StaleBaseError) stale += 1;
          else if (err instanceof LeaseExpiredError) expired += 1;
          else if (err instanceof NoLeaseError) noLease += 1;
          else throw err;
        }
      }
    }
  } finally {
    hub.close();
  }
  process.stdout.write(
    JSON.stringify({ applied, leaseConflicts, stale, expired, noLease }) + "\n",
  );
}

main();
