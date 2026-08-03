/**
 * Compaction worker: repeatedly runs compact() on a shared SQLite session while
 * other processes write revisions and a consumer reads. Because compact() takes
 * the IMMEDIATE write lock and never touches segments/events, it is safe to run
 * concurrently with writers. Emits one JSON line summarising its passes.
 *
 * Args: <dbPath> <sessionId> <passes> <sleepMs>
 * Emits: {"passes":n,"reclaimedTotal":n,"maxWatermark":n}
 */
import { RevisionHub } from "../../src/index";

function sleepBusy(ms: number): void {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    /* keep the process scheduling fair between passes */
  }
}

function main(): void {
  const [dbPath, sessionId, passes, sleepMs] = process.argv.slice(2);
  const hub = RevisionHub.open({ path: dbPath!, busyTimeoutMs: 20000 });
  let reclaimedTotal = 0;
  let maxWatermark = 0;
  try {
    for (let i = 0; i < Number(passes); i++) {
      const stats = hub.compact(sessionId!);
      reclaimedTotal += stats.reclaimed;
      if (stats.compactedUpto > maxWatermark) maxWatermark = stats.compactedUpto;
      if (Number(sleepMs) > 0) sleepBusy(Number(sleepMs));
    }
  } finally {
    hub.close();
  }
  process.stdout.write(JSON.stringify({ passes: Number(passes), reclaimedTotal, maxWatermark }) + "\n");
}

main();
