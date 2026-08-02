import { describe, expect, it } from 'vitest';
import { ReviewConflictError, TranscriptStore, ValidationError } from '../src';
import type { AsrEvent } from '../src';
import { tmpDbPath } from './helpers';

const fin = (seq: number, text: string, id = `e${seq}`): AsrEvent => ({
  eventId: id,
  sourceSeq: seq,
  kind: 'final',
  text,
  startMs: seq * 10,
});

/** Seed two final segments; returns the session's last revision. */
function seed(s: TranscriptStore): number {
  s.ingest('sess', 'mic', fin(0, 'hello world'));
  s.ingest('sess', 'mic', fin(1, 'goodbye world'));
  return s.lastRevision('sess');
}

function reasonOf(fn: () => unknown): string {
  try {
    fn();
  } catch (e) {
    if (e instanceof ReviewConflictError) return e.reason;
    throw e;
  }
  throw new Error('expected ReviewConflictError, but the call succeeded');
}

describe('review leases & corrections', () => {
  it('happy path: lease, correct, snapshot shows effective text and lineage', () => {
    const s = new TranscriptStore(':memory:');
    const base = seed(s);
    const target = { sourceId: 'mic', eventId: 'e0' };

    const lease = s.acquireLease('sess', target, { actor: 'alice', baseRevision: base, ttlMs: 60_000 });
    expect(lease.leaseId).toBeTruthy();

    const r = s.submitCorrection('sess', target, {
      leaseId: lease.leaseId,
      baseRevision: base,
      text: 'hello, world!',
      actor: 'alice',
      reason: 'punctuation',
    });
    expect(r).toMatchObject({ status: 'applied', revision: base + 1 });

    const snap = s.snapshot('sess');
    const seg = snap.segments.find((g) => g.eventId === 'e0')!;
    expect(seg.text).toBe('hello, world!');
    expect(seg.originalText).toBe('hello world');
    expect(seg.correction).toMatchObject({
      correctionId: r.correctionId,
      actor: 'alice',
      reason: 'punctuation',
      revision: base + 1,
      supersedes: null,
    });
    expect(snap.text).toBe('hello, world! goodbye world');
    expect(snap.summary.corrections).toBe(1);
    // lease consumed
    expect(s.lease('sess', target)).toBeNull();
    s.close();
  });

  it('corrections join the same durable revision stream for existing consumers', () => {
    const s = new TranscriptStore(':memory:');
    const base = seed(s);
    s.poll('qc', 'sess', 100); // existing consumer saw everything
    s.ack('qc', 'sess', base);

    const lease = s.acquireLease('sess', { sourceId: 'mic', eventId: 'e1' }, {
      actor: 'bob', baseRevision: base, ttlMs: 60_000,
    });
    const r = s.submitCorrection('sess', { sourceId: 'mic', eventId: 'e1' }, {
      leaseId: lease.leaseId, baseRevision: base, text: 'goodbye, world!', actor: 'bob', reason: 'punct',
    });

    const { entries } = s.poll('qc', 'sess', 100);
    expect(entries).toHaveLength(1);
    expect(entries[0].revision).toBe(r.revision);
    expect(entries[0].change).toMatchObject({
      type: 'correction',
      sourceId: 'mic',
      eventId: 'e1',
      text: 'goodbye, world!',
      actor: 'bob',
      reason: 'punct',
      supersedes: null,
    });
    s.close();
  });

  it('competing acquisition: exactly one reviewer wins', () => {
    const s = new TranscriptStore(':memory:');
    const base = seed(s);
    const target = { sourceId: 'mic', eventId: 'e0' };
    s.acquireLease('sess', target, { actor: 'alice', baseRevision: base, ttlMs: 60_000 });
    expect(
      reasonOf(() =>
        s.acquireLease('sess', target, { actor: 'bob', baseRevision: base, ttlMs: 60_000 }),
      ),
    ).toBe('lease_held');
    s.close();
  });

  it('submit without/with wrong lease fails as no_lease', () => {
    const s = new TranscriptStore(':memory:');
    const base = seed(s);
    const target = { sourceId: 'mic', eventId: 'e0' };
    const opts = { leaseId: 'nope', baseRevision: base, text: 'x', actor: 'a', reason: 'r' };
    expect(reasonOf(() => s.submitCorrection('sess', target, opts))).toBe('no_lease');

    const lease = s.acquireLease('sess', target, { actor: 'a', baseRevision: base, ttlMs: 60_000 });
    expect(reasonOf(() => s.submitCorrection('sess', target, opts))).toBe('no_lease');
    expect(
      s.submitCorrection('sess', target, { ...opts, leaseId: lease.leaseId }).status,
    ).toBe('applied');
    // lease is consumed: resubmission fails
    expect(reasonOf(() => s.submitCorrection('sess', target, { ...opts, leaseId: lease.leaseId }))).toBe(
      'no_lease',
    );
    s.close();
  });

  it('expired lease: submission conflicts and the segment can be re-leased', () => {
    let now = 1_000;
    const s = new TranscriptStore(':memory:', { now: () => now });
    const base = seed(s);
    const target = { sourceId: 'mic', eventId: 'e0' };

    const lease = s.acquireLease('sess', target, { actor: 'alice', baseRevision: base, ttlMs: 100 });
    now += 150; // lease expired
    expect(
      reasonOf(() =>
        s.submitCorrection('sess', target, {
          leaseId: lease.leaseId, baseRevision: base, text: 'x', actor: 'alice', reason: 'r',
        }),
      ),
    ).toBe('lease_expired');

    // after expiry another reviewer may acquire and submit
    const lease2 = s.acquireLease('sess', target, { actor: 'bob', baseRevision: base, ttlMs: 100 });
    expect(lease2.leaseId).not.toBe(lease.leaseId);
    now += 50;
    expect(
      s.submitCorrection('sess', target, {
        leaseId: lease2.leaseId, baseRevision: base, text: 'fixed', actor: 'bob', reason: 'r',
      }).status,
    ).toBe('applied');
    s.close();
  });

  it('stale base is rejected both at acquire and at submit', () => {
    const s = new TranscriptStore(':memory:');
    const base = seed(s);
    const target = { sourceId: 'mic', eventId: 'e0' };

    // first correction at revision base+1
    const l1 = s.acquireLease('sess', target, { actor: 'alice', baseRevision: base, ttlMs: 60_000 });
    s.submitCorrection('sess', target, {
      leaseId: l1.leaseId, baseRevision: base, text: 'v2', actor: 'alice', reason: 'r',
    });

    // acquiring on the old base now conflicts immediately
    expect(
      reasonOf(() => s.acquireLease('sess', target, { actor: 'bob', baseRevision: base, ttlMs: 60_000 })),
    ).toBe('stale_base');

    // and submitting with a base that does not match the lease conflicts too
    const l2 = s.acquireLease('sess', target, { actor: 'bob', baseRevision: base + 1, ttlMs: 60_000 });
    expect(
      reasonOf(() =>
        s.submitCorrection('sess', target, {
          leaseId: l2.leaseId, baseRevision: base, text: 'v3', actor: 'bob', reason: 'r',
        }),
      ),
    ).toBe('stale_base');
    // the lease is still live after the failed submit; right base succeeds
    expect(
      s.submitCorrection('sess', target, {
        leaseId: l2.leaseId, baseRevision: base + 1, text: 'v3', actor: 'bob', reason: 'r',
      }).status,
    ).toBe('applied');
    s.close();
  });

  it('supersedes lineage chains corrections', () => {
    const s = new TranscriptStore(':memory:');
    const base = seed(s);
    const target = { sourceId: 'mic', eventId: 'e0' };

    const l1 = s.acquireLease('sess', target, { actor: 'a', baseRevision: base, ttlMs: 60_000 });
    const c1 = s.submitCorrection('sess', target, {
      leaseId: l1.leaseId, baseRevision: base, text: 'v2', actor: 'a', reason: 'first',
    });
    const l2 = s.acquireLease('sess', target, { actor: 'b', baseRevision: c1.revision, ttlMs: 60_000 });
    const c2 = s.submitCorrection('sess', target, {
      leaseId: l2.leaseId, baseRevision: c1.revision, text: 'v3', actor: 'b', reason: 'second',
    });

    const seg = s.snapshot('sess').segments.find((g) => g.eventId === 'e0')!;
    expect(seg.text).toBe('v3');
    expect(seg.correction).toMatchObject({ correctionId: c2.correctionId, supersedes: c1.correctionId });

    // both corrections are in the stream, in order
    const { entries } = s.poll('c', 'sess', 100);
    const corr = entries.filter((e) => e.change.type === 'correction');
    expect(corr.map((e) => e.change.correctionId)).toEqual([c1.correctionId, c2.correctionId]);
    expect(corr[1].change.supersedes).toBe(c1.correctionId);
    s.close();
  });

  it('correction does not rewrite the original event nor relax final/stale rules', () => {
    const s = new TranscriptStore(':memory:');
    const base = seed(s);
    const target = { sourceId: 'mic', eventId: 'e0' };
    const lease = s.acquireLease('sess', target, { actor: 'a', baseRevision: base, ttlMs: 60_000 });
    s.submitCorrection('sess', target, {
      leaseId: lease.leaseId, baseRevision: base, text: 'fixed', actor: 'a', reason: 'r',
    });

    // duplicate of the original event is still a duplicate, content unchanged
    expect(s.ingest('sess', 'mic', fin(0, 'hello world')).status).toBe('duplicate');
    // a late partial still cannot roll back the final
    expect(
      s.ingest('sess', 'mic', { eventId: 'p9', sourceSeq: 0, kind: 'partial', text: 'he' }).status,
    ).toBe('stale');
    const seg = s.snapshot('sess').segments.find((g) => g.eventId === 'e0')!;
    expect(seg.originalText).toBe('hello world');
    s.close();
  });

  it('cannot lease a partial or a missing segment', () => {
    const s = new TranscriptStore(':memory:');
    s.ingest('sess', 'mic', { eventId: 'p0', sourceSeq: 0, kind: 'partial', text: 'he' });
    expect(() =>
      s.acquireLease('sess', { sourceId: 'mic', eventId: 'p0' }, { actor: 'a', baseRevision: 1, ttlMs: 1000 }),
    ).toThrow(ValidationError);
    expect(() =>
      s.acquireLease('sess', { sourceId: 'mic', eventId: 'nope' }, { actor: 'a', baseRevision: 1, ttlMs: 1000 }),
    ).toThrow(ValidationError);
    s.close();
  });

  it('release frees the segment for another reviewer', () => {
    const s = new TranscriptStore(':memory:');
    const base = seed(s);
    const target = { sourceId: 'mic', eventId: 'e0' };
    const l = s.acquireLease('sess', target, { actor: 'a', baseRevision: base, ttlMs: 60_000 });
    expect(s.releaseLease('sess', target, l.leaseId)).toBe(true);
    expect(s.releaseLease('sess', target, l.leaseId)).toBe(false);
    expect(() =>
      s.acquireLease('sess', target, { actor: 'b', baseRevision: base, ttlMs: 60_000 }),
    ).not.toThrow();
    s.close();
  });

  it('leases and corrections survive restart; competing stores agree on a single winner', () => {
    const file = tmpDbPath();
    const a = new TranscriptStore(file);
    const base = seed(a);
    const target = { sourceId: 'mic', eventId: 'e0' };

    // two instances race for the same lease
    const b = new TranscriptStore(file);
    const results = [a, b].map((store, i) => {
      try {
        return store.acquireLease('sess', target, { actor: `r${i}`, baseRevision: base, ttlMs: 60_000 });
      } catch (e) {
        if (e instanceof ReviewConflictError) return e.reason;
        throw e;
      }
    });
    expect(results.filter((r) => typeof r === 'object')).toHaveLength(1);
    expect(results.filter((r) => r === 'lease_held')).toHaveLength(1);

    const winner = results[0] && typeof results[0] === 'object' ? a : b;
    const lease = (typeof results[0] === 'object' ? results[0] : results[1]) as { leaseId: string };
    a.close();
    b.close();

    // restart: lease still held, submission against it works, stream continues
    const c = new TranscriptStore(file);
    expect(c.lease('sess', target)?.leaseId).toBe(lease.leaseId);
    expect(
      reasonOf(() => c.acquireLease('sess', target, { actor: 'late', baseRevision: base, ttlMs: 60_000 })),
    ).toBe('lease_held');
    void winner;
    const r = c.submitCorrection('sess', target, {
      leaseId: lease.leaseId, baseRevision: base, text: 'after restart', actor: 'r', reason: 'resume',
    });
    expect(r.revision).toBe(base + 1);

    // consumer that acked everything before the crash sees exactly the correction
    c.ack('qc', 'sess', base);
    const { entries } = c.poll('qc', 'sess', 100);
    expect(entries.map((e) => e.change.type)).toEqual(['correction']);
    const snap = c.snapshot('sess');
    expect(snap.segments.find((g) => g.eventId === 'e0')!.text).toBe('after restart');
    c.close();

    // and it persists across yet another reopen
    const d = new TranscriptStore(file);
    expect(d.snapshot('sess').segments.find((g) => g.eventId === 'e0')!.text).toBe('after restart');
    expect(d.poll('qc2', 'sess', 100).entries.at(-1)!.change.type).toBe('correction');
    d.close();
  });
});
