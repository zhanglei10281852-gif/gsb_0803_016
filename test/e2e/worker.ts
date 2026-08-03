/**
 * Worker child process for the multi-instance concurrency e2e.
 *
 * Each worker opens the SAME SQLite file as its own RevisionHub instance and
 * applies its assigned (shuffled) slice of the shared event stream. Multiple
 * workers run at once, so this exercises cross-process write contention,
 * revision allocation under load, and idempotency of overlapping deliveries.
 *
 * Args: <dbPath> <sessionId> <sources> <segs> <partials> <workerIndex> <workerCount> <seed> <overlap>
 * Emits a single JSON line on stdout: {"applied":n,"effective":n,"conflicts":n}
 */
import { RevisionHub, ConflictError } from "../../src/index";
import { makeEvents, shuffle, rng } from "./shared";

function main(): void {
  const [dbPath, sessionId, sources, segs, partials, wIdx, wCount, seed, overlap] =
    process.argv.slice(2);

  const all = makeEvents(sessionId!, Number(sources), Number(segs), Number(partials));
  const idx = Number(wIdx);
  const count = Number(wCount);
  const ov = Number(overlap);

  // Partition round-robin, then optionally duplicate a fraction across workers
  // so the same event is delivered by more than one process (must be idempotent).
  const rand = rng(Number(seed) + idx * 7919);
  let mine = all.filter((_, i) => i % count === idx);
  if (ov > 0) {
    const extra = all.filter(() => rand() < ov);
    mine = mine.concat(extra);
  }
  mine = shuffle(mine, rand);

  const hub = RevisionHub.open({ path: dbPath!, busyTimeoutMs: 15000 });
  let applied = 0;
  let effective = 0;
  let conflicts = 0;
  try {
    for (const ev of mine) {
      try {
        const res = hub.apply(ev);
        applied += 1;
        if (res.effective) effective += 1;
      } catch (err) {
        if (err instanceof ConflictError) conflicts += 1;
        else throw err;
      }
    }
  } finally {
    hub.close();
  }
  process.stdout.write(JSON.stringify({ applied, effective, conflicts }) + "\n");
}

main();
