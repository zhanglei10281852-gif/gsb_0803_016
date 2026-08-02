/**
 * Crash worker: applies the shared event stream one event at a time, printing
 * a progress line ("applied <n>") after each commit and flushing so the parent
 * can SIGKILL it mid-stream to emulate power loss. Because every apply is a
 * single durable (WAL + synchronous=FULL) IMMEDIATE transaction, a hard kill
 * must leave the store at a clean event boundary — never a half-written change.
 *
 * Args: <dbPath> <sessionId> <sources> <segs> <partials> <delayMs>
 */
import { RevisionHub } from "../../src/index";
import { makeEvents } from "./shared";

function sleepBusy(ms: number): void {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    /* busy wait keeps the event loop from exiting between writes */
  }
}

function main(): void {
  const [dbPath, sessionId, sources, segs, partials, delayMs] = process.argv.slice(2);
  const events = makeEvents(sessionId!, Number(sources), Number(segs), Number(partials));
  const hub = RevisionHub.open({ path: dbPath!, synchronous: "FULL", busyTimeoutMs: 15000 });
  let n = 0;
  try {
    for (const ev of events) {
      hub.apply(ev);
      n += 1;
      process.stdout.write(`applied ${n}\n`);
      if (Number(delayMs) > 0) sleepBusy(Number(delayMs));
    }
  } finally {
    hub.close();
  }
}

main();
