import fs from 'node:fs';
import { TranscriptStore } from '../dist/index.js';

/**
 * Child process: commits `batches` batches of `batchSize` events each.
 * After every committed batch it prints "BATCH <i>" and blocks until the
 * parent sends "go" on stdin, so the parent can SIGKILL it at an exact
 * batch boundary with a deterministic committed prefix.
 */
const [dbPath, batchesArg, batchSizeArg] = process.argv.slice(2);
const batches = Number(batchesArg);
const batchSize = Number(batchSizeArg);

export function batchEvents(batchIndex, size) {
  const events = [];
  for (let j = 0; j < size; j++) {
    const seq = batchIndex * size + j;
    events.push({
      eventId: `e${seq}`,
      sourceSeq: seq,
      kind: 'final',
      text: `segment ${seq}`,
      startMs: seq * 10,
    });
  }
  return events;
}

function waitForGo() {
  const buf = Buffer.alloc(16);
  fs.readSync(0, buf, 0, 16, null); // blocking read on stdin
}

function run() {
  const store = new TranscriptStore(dbPath);
  for (let i = 0; i < batches; i++) {
    store.ingestBatch('sess', 'mic', batchEvents(i, batchSize));
    fs.writeSync(1, `BATCH ${i}\n`);
    waitForGo();
  }
  store.close();
  fs.writeSync(1, 'DONE\n');
  process.exit(0);
}

// Only run as a child process; crash.mjs imports batchEvents from this file.
if (process.argv[1] && process.argv[1].endsWith('crash-child.mjs')) {
  run();
}
