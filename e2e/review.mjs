import assert from 'node:assert/strict';
import { ReviewConflictError, TranscriptStore } from '../dist/index.js';
import { mulberry32, sleep, tmpDbPath } from './lib.mjs';

const SEGMENTS = 6;
const ROUNDS = 40;

/**
 * Human review flow under concurrency and restart: reviewers on two store
 * instances compete for segment leases, submit corrections, lose leases to
 * expiry, and resume after a full restart. Invariants:
 *  - at most one live lease per segment (losers get lease_held);
 *  - every committed correction is a contiguous revision in the same stream,
 *    chaining via supersedes;
 *  - the snapshot after restart reflects the last correction per segment.
 */
export async function main() {
  const file = tmpDbPath('asr-e2e-review-');
  const rng = mulberry32(555);

  const setup = new TranscriptStore(file);
  const targets = [];
  for (let i = 0; i < SEGMENTS; i++) {
    setup.ingest('sess', 'mic', {
      eventId: `e${i}`,
      sourceSeq: i,
      kind: 'final',
      text: `original ${i}`,
      startMs: i * 100,
    });
    targets.push({ sourceId: 'mic', eventId: `e${i}` });
  }
  const seedRevision = setup.lastRevision('sess');
  setup.close();

  const stores = [new TranscriptStore(file), new TranscriptStore(file)];
  const successes = []; // {target, correctionId, revision, text} in commit order
  let held = 0;
  let expired = 0;

  const reviewers = stores.map(async (store, w) => {
    for (let r = 0; r < ROUNDS; r++) {
      const target = targets[Math.floor(rng() * SEGMENTS)];
      const actor = `reviewer-${w}`;
      const baseRevision = store.lastRevision('sess');
      try {
        const lease = store.acquireLease('sess', target, {
          actor,
          baseRevision,
          ttlMs: 30 + Math.floor(rng() * 60),
        });
        if (rng() < 0.15) await sleep(120); // dawdle: let the lease expire
        try {
          const res = store.submitCorrection('sess', target, {
            leaseId: lease.leaseId,
            baseRevision,
            text: `fixed-by-${actor}-@${r}`,
            actor,
            reason: 'e2e',
          });
          successes.push({ target, revision: res.revision, correctionId: res.correctionId, text: `fixed-by-${actor}-@${r}` });
        } catch (e) {
          assert.ok(e instanceof ReviewConflictError, 'submit conflict type');
          assert.ok(['lease_expired', 'stale_base', 'no_lease'].includes(e.reason), e.reason);
          expired++;
        }
      } catch (e) {
        assert.ok(e instanceof ReviewConflictError, 'acquire conflict type');
        assert.ok(['lease_held', 'stale_base'].includes(e.reason), e.reason);
        held++;
      }
      if (r % 5 === 0) await sleep(0);
    }
  });
  await Promise.all(reviewers);
  assert.ok(successes.length > 0, 'some corrections committed');
  assert.ok(held > 0, 'lease contention actually happened');
  for (const s of stores) s.close();

  // Restart, then verify from a fresh instance.
  const store = new TranscriptStore(file);

  // Stream: contiguous, seed prefix untouched, corrections in commit order.
  const { entries } = store.poll('audit', 'sess', 10000);
  assert.deepEqual(
    entries.map((e) => e.revision),
    Array.from({ length: store.lastRevision('sess') }, (_, i) => i + 1),
  );
  const corrEntries = entries.filter((e) => e.change.type === 'correction');
  assert.deepEqual(
    corrEntries.map((e) => e.revision),
    [...successes.map((s) => s.revision)].sort((a, b) => a - b),
  );
  // supersedes chain per segment follows commit order
  const lastCorr = new Map();
  for (const s of successes) {
    const prev = lastCorr.get(`${s.target.sourceId}/${s.target.eventId}`);
    const entry = corrEntries.find((e) => e.revision === s.revision);
    assert.equal(entry.change.supersedes ?? null, prev?.correctionId ?? null);
    lastCorr.set(`${s.target.sourceId}/${s.target.eventId}`, s);
  }
  // ASR events were never rewritten
  assert.equal(
    entries.filter((e) => e.change.type !== 'correction').length,
    seedRevision,
  );

  // Snapshot after restart: effective text equals the last correction.
  const snap = store.snapshot('sess');
  for (const seg of snap.segments) {
    const last = lastCorr.get(`${seg.sourceId}/${seg.eventId}`);
    if (last) {
      assert.equal(seg.text, last.text);
      assert.equal(seg.originalText, `original ${seg.eventId.slice(1)}`);
      assert.equal(seg.correction.actor.startsWith('reviewer-'), true);
    } else {
      assert.equal(seg.text, `original ${seg.eventId.slice(1)}`);
      assert.equal(seg.correction, undefined);
    }
  }
  assert.equal(snap.summary.corrections, lastCorr.size);

  // Review continues after restart: lease + correction still work.
  const t0 = targets[0];
  const base = store.lastRevision('sess');
  const lease = store.acquireLease('sess', t0, { actor: 'post-restart', baseRevision: base, ttlMs: 5000 });
  const res = store.submitCorrection('sess', t0, {
    leaseId: lease.leaseId,
    baseRevision: base,
    text: 'post-restart fix',
    actor: 'post-restart',
    reason: 'resume',
  });
  assert.equal(res.revision, base + 1);
  assert.equal(store.snapshot('sess').segments.find((g) => g.eventId === t0.eventId).text, 'post-restart fix');

  store.close();
  console.log(
    `e2e/review: OK (${successes.length} corrections, ${held} lease conflicts, ${expired} expired/stale submits)`,
  );
}

if (process.argv[1] && process.argv[1].endsWith('review.mjs')) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
