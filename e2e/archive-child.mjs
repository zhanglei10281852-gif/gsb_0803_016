import readline from 'node:readline';
import { TranscriptStore } from '../dist/index.js';

/**
 * Child process: imports an archive fed line-by-line over stdin (the parent
 * controls the pace and may SIGKILL mid-stream). Prints IMPORTED on success.
 */
const [dbPath] = process.argv.slice(2);

async function* stdinLines() {
  const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of rl) yield line;
}

const store = new TranscriptStore(dbPath);
const result = await store.importArchive(stdinLines());
store.close();
process.stdout.write(`IMPORTED ${result.sessionId} ${result.lastRevision}\n`);
process.exit(0);
