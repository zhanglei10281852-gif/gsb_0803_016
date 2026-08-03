import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { TranscriptStore } from '../dist/index.js';
import { mulberry32, tmpDbPath } from './lib.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));

/**
 * Compliance handoff: export a reviewed session, then verify
 *  1. import into an empty database is fully equivalent (snapshot, stream,
 *     cursors, correction lineage);
 *  2. a power cut mid-import leaves no half-imported session, and the same
 *     archive imports cleanly afterwards (idempotent on top of that);
 *  3. a tampered archive is rejected with nothing visible.
 */
export async function main() {
  // --- build a source session with corrections and cursors ---
  const srcFile = tmpDbPath('asr-e2e-arch-src-');
  const src = new TranscriptStore(srcFile);
  const rng = mulberry32(808);
  for (let i = 0; i < 40; i++) {
    src.ingest('sess', `mic-${i % 2}`, {
      eventId: `e${i}`,
      sourceSeq: Math.floor(i / 2),
      kind: i % 3 === 2 ? 'final' : 'partial',
      text: `segment ${i}`,
      startMs: i * 50,
    });
  }
  const target = { sourceId: 'mic-1', eventId: 'e5' };
  const base = src.lastRevision('sess');
  const l1 = src.acquireLease('sess', target, { actor: 'alice', baseRevision: base, ttlMs: 60_000 });
  const c1 = src.submitCorrection('sess', target, {
    leaseId: l1.leaseId, baseRevision: base, text: 'fixed 2', actor: 'alice', reason: 'typo',
  });
  const l2 = src.acquireLease('sess', target, { actor: 'bob', baseRevision: c1.revision, ttlMs: 60_000 });
  src.submitCorrection('sess', target, {
    leaseId: l2.leaseId, baseRevision: c1.revision, text: 'fixed 2 final', actor: 'bob', reason: 'sign-off',
  });
  src.ack('qc', 'sess', 10);
  src.ack('audit', 'sess', src.lastRevision('sess'));

  const lines = [];
  for await (const line of src.exportArchive('sess')) lines.push(line.replace(/\n$/, ''));
  const archiveText = lines.join('\n') + '\n';
  const wantSnap = src.snapshot('sess');
  const wantStream = src.poll('full', 'sess', 10000).entries;
  src.close();

  // --- 1. import into empty DB: full equivalence ---
  const dstFile = tmpDbPath('asr-e2e-arch-dst-');
  const dst = new TranscriptStore(dstFile);
  const r1 = await dst.importArchive(archiveText);
  assert.equal(r1.status, 'imported');
  assert.deepEqual(dst.snapshot('sess'), wantSnap);
  assert.deepEqual(dst.poll('full', 'sess', 10000).entries, wantStream);
  assert.equal(dst.cursor('qc', 'sess'), 10);
  const corrSeg = dst.snapshot('sess').segments.find((s) => s.eventId === 'e5');
  assert.equal(corrSeg.text, 'fixed 2 final');
  assert.equal(corrSeg.correction.actor, 'bob');
  assert.equal(typeof corrSeg.correction.supersedes, 'string');
  // idempotent re-import
  const r2 = await dst.importArchive(archiveText);
  assert.equal(r2.status, 'duplicate');
  assert.deepEqual(dst.snapshot('sess'), wantSnap);
  dst.close();

  // --- 2. power cut mid-import: nothing visible, then clean re-import ---
  const cutFile = tmpDbPath('asr-e2e-arch-cut-');
  const child = spawn(process.execPath, [join(__dirname, 'archive-child.mjs'), cutFile], {
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  child.stdin.on('error', () => {}); // expected EPIPE after the kill
  const half = Math.floor(lines.length / 2);
  for (let i = 0; i < half; i++) child.stdin.write(lines[i] + '\n');
  child.kill('SIGKILL');
  await new Promise((resolve) => child.once('close', resolve));

  const after = new TranscriptStore(cutFile);
  assert.equal(after.lastRevision('sess'), 0, 'no half-imported session after power cut');
  assert.equal(after.snapshot('sess').segments.length, 0);
  const r3 = await after.importArchive(archiveText);
  assert.equal(r3.status, 'imported');
  assert.deepEqual(after.snapshot('sess'), wantSnap);
  const r4 = await after.importArchive(archiveText);
  assert.equal(r4.status, 'duplicate');
  after.close();

  // --- 3. tampered archive rejected ---
  const tampered = lines.map((l) => {
    const rec = JSON.parse(l);
    if (rec.type === 'correction') rec.actor = 'mallory';
    return JSON.stringify(rec);
  });
  const dst2 = new TranscriptStore(tmpDbPath('asr-e2e-arch-tam-'));
  await assert.rejects(dst2.importArchive(tampered.join('\n') + '\n'));
  assert.equal(dst2.lastRevision('sess'), 0);
  dst2.close();

  console.log(`e2e/archive: OK (${lines.length} lines, equivalence + power-cut + tamper verified)`);
}

if (process.argv[1] && process.argv[1].endsWith('archive.mjs')) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
