/**
 * Correction crash worker: for each segment it acquires a lease and submits a
 * correction, printing "done <n>" after each committed correction so the parent
 * can SIGKILL it mid-stream. Because acquire and submit are each single durable
 * IMMEDIATE transactions, a hard kill must leave the store at a clean boundary:
 * a correction's segment flip, lineage row, and stream revision either all
 * committed together or not at all.
 *
 * Args: <dbPath> <sessionId> <segCount> <actor> <delayMs>
 */
import { RevisionHub } from "../../src/index";

function sleepBusy(ms: number): void {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    /* keep process alive between writes */
  }
}

function main(): void {
  const [dbPath, sessionId, segCount, actor, delayMs] = process.argv.slice(2);
  const hub = RevisionHub.open({ path: dbPath!, synchronous: "FULL", busyTimeoutMs: 20000 });
  let done = 0;
  try {
    for (let g = 0; g < Number(segCount); g++) {
      const segmentId = `seg-${g}`;
      const snap = hub.getSnapshot(sessionId!);
      const seg = snap.segments.find((s) => s.segmentId === segmentId);
      if (!seg) continue;
      const lease = hub.acquireLease({
        sessionId: sessionId!,
        sourceId: "src-0",
        segmentId,
        actor: actor!,
        baseRevision: seg.revision,
        ttlMs: 60000,
      });
      hub.submitCorrection({
        leaseId: lease.leaseId,
        actor: actor!,
        text: `${actor} corrected ${segmentId}`,
        reason: "review",
        correctionId: `${actor}:${segmentId}`,
      });
      done += 1;
      process.stdout.write(`done ${done}\n`);
      if (Number(delayMs) > 0) sleepBusy(Number(delayMs));
    }
  } finally {
    hub.close();
  }
}

main();
