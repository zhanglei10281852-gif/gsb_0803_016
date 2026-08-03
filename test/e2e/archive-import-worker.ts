/**
 * Archive import crash worker. Reads a session archive from a file and imports
 * it into a target SQLite DB, but is designed to be SIGKILL-ed mid-import by the
 * parent. It prints "start" once opened, then "committed" only after the single
 * import transaction returns. Because import runs in one IMMEDIATE transaction,
 * a kill before "committed" must leave the target with NO visible session.
 *
 * Args: <targetDbPath> <archiveFilePath> <sessionId> <holdMs>
 */
import { readFileSync } from "node:fs";
import { RevisionHub } from "../../src/index";

function sleepBusy(ms: number): void {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    /* keep alive so the parent can kill us mid-flight */
  }
}

function main(): void {
  const [target, archivePath, sessionId, holdMs] = process.argv.slice(2);
  const archive = readFileSync(archivePath!, "utf8");
  const hub = RevisionHub.open({ path: target!, busyTimeoutMs: 20000 });
  try {
    process.stdout.write("start\n");
    // If holdMs > 0 we linger BEFORE importing, giving the parent a window to
    // kill us so we can prove a killed import leaves nothing behind.
    if (Number(holdMs) > 0) sleepBusy(Number(holdMs));
    const res = hub.importSession(archive);
    process.stdout.write(`committed ${res.imported} ${res.headRevision}\n`);
  } finally {
    hub.close();
  }
}

main();
