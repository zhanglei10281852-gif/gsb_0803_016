import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import readline from 'node:readline';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { ReferenceModel, TranscriptStore } from '../dist/index.js';
import { tmpDbPath } from './lib.mjs';
import { batchEvents } from './crash-child.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const BATCHES = 12;
const BATCH_SIZE = 5;
const KILL_AFTER = 4;

/**
 * Power-cut simulation: a producer process is SIGKILLed mid-stream. After
 * reopening, the database must contain only whole committed batches (no
 * half-written state), revisions must be contiguous, and a new process must
 * be able to resume and converge to the full reference model.
 */
export async function main() {
  const file = tmpDbPath('asr-e2e-crash-');

  const child = spawn(
    process.execPath,
    [join(__dirname, 'crash-child.mjs'), file, String(BATCHES), String(BATCH_SIZE)],
    { stdio: ['pipe', 'pipe', 'inherit'] },
  );
  child.stdin.setDefaultEncoding('utf8');

  const rl = readline.createInterface({ input: child.stdout });
  let seen = 0;
  for await (const line of rl) {
    if (line.startsWith('BATCH')) {
      seen++;
      if (seen === KILL_AFTER) {
        child.kill('SIGKILL'); // power cut: no cleanup, no atexit
        break;
      }
      child.stdin.write('go\n');
    }
  }
  await new Promise((resolve) => child.once('close', resolve));
  assert.ok(child.signalCode === 'SIGKILL' || child.exitCode !== 0, 'child was killed');

  // Reopen and verify: only whole batches, contiguous revisions 1..N,
  // every revision's change matches the deterministic event for that slot.
  const store = new TranscriptStore(file);
  const last = store.lastRevision('sess');
  assert.equal(last, KILL_AFTER * BATCH_SIZE, 'exactly the committed prefix survived');
  const { entries } = store.poll('checker', 'sess', 10000);
  assert.equal(entries.length, last);
  for (const e of entries) {
    assert.equal(e.change.text, `segment ${e.revision - 1}`);
    assert.equal(e.change.sourceSeq, e.revision - 1);
  }

  // Resume in the "restarted" process and finish the remaining batches.
  const model = new ReferenceModel();
  for (let i = 0; i < BATCHES; i++) {
    for (const ev of batchEvents(i, BATCH_SIZE)) model.ingest('mic', ev);
  }
  for (let i = KILL_AFTER; i < BATCHES; i++) {
    store.ingestBatch('sess', 'mic', batchEvents(i, BATCH_SIZE));
  }
  const { sessionId: _a, ...snap } = store.snapshot('sess');
  const { sessionId: _b, ...want } = model.snapshot('sess');
  assert.deepEqual(snap, want);
  assert.equal(store.lastRevision('sess'), BATCHES * BATCH_SIZE);
  store.close();
  console.log(`e2e/crash: OK (killed after ${KILL_AFTER}/${BATCHES} batches, resumed cleanly)`);
}

if (process.argv[1] && process.argv[1].endsWith('crash.mjs')) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
