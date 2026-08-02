import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTempStore, makeEvent } from '../helpers';
import {
  RecognitionStore,
  LeaseBusyError,
  LeaseExpiredError,
  LeaseConsumedError,
  LeaseNotFoundError,
  StaleBaseRevisionError,
  SlotFinalizedError,
} from '../../src';

describe('correction lease workflow', () => {
  let store: RecognitionStore;
  let cleanup: () => void;

  beforeEach(() => {
    const ctx = createTempStore();
    store = ctx.store;
    cleanup = ctx.cleanup;
  });
  afterEach(() => cleanup());

  function setupFinalFragment(sessionId = 's1', content = 'helo world') {
    const session = store.session(sessionId);
    session.ingest(makeEvent('srcA', 1, 'final', content, 'e1'));
    return session;
  }

  it('acquires a lease on an existing fragment', () => {
    const session = setupFinalFragment();
    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
      ttlMs: 30000,
    });
    expect(lease.leaseId).toBeTruthy();
    expect(lease.actor).toBe('reviewer-1');
    expect(lease.baseContent).toBe('helo world');
    expect(lease.status).toBe('active');
    expect(lease.expiresAt).toBeGreaterThan(lease.acquiredAt);
  });

  it('rejects acquiring a lease when another active lease exists', () => {
    const session = setupFinalFragment();
    session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
    });
    expect(() =>
      session.acquireLease({
        sourceId: 'srcA',
        sourceSeq: 1,
        actor: 'reviewer-2',
        baseRevision: 1,
      }),
    ).toThrow(LeaseBusyError);
  });

  it('rejects acquiring a lease on a non-existent fragment', () => {
    const session = store.session('empty');
    expect(() =>
      session.acquireLease({
        sourceId: 'srcX',
        sourceSeq: 99,
        actor: 'reviewer-1',
        baseRevision: 0,
      }),
    ).toThrow(LeaseNotFoundError);
  });

  it('allows acquiring a lease after the previous one expires', async () => {
    const session = setupFinalFragment();
    session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
      ttlMs: 50,
    });
    await new Promise((r) => setTimeout(r, 80));
    const lease2 = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-2',
      baseRevision: 1,
    });
    expect(lease2.actor).toBe('reviewer-2');
  });

  it('submits a correction and creates a new revision', () => {
    const session = setupFinalFragment();
    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
    });
    const correction = session.submitCorrection({
      leaseId: lease.leaseId,
      correctedContent: 'hello world',
      reason: 'typo: helo -> hello',
    });

    expect(correction.correctionId).toBeTruthy();
    expect(correction.originalContent).toBe('helo world');
    expect(correction.correctedContent).toBe('hello world');
    expect(correction.actor).toBe('reviewer-1');
    expect(correction.supersedesCorrectionId).toBeNull();
    expect(correction.revision).toBe(2);

    const snap = session.getSnapshot();
    expect(snap.text).toBe('hello world');
    expect(snap.revision).toBe(2);
    expect(snap.summary.correctedCount).toBe(1);
    expect(snap.sources[0].fragments[0].corrected).toBe(true);
  });

  it('correction does not rewrite the original recognition event', () => {
    const session = setupFinalFragment();
    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
    });
    session.submitCorrection({
      leaseId: lease.leaseId,
      correctedContent: 'hello world',
      reason: 'fix',
    });

    const corrections = session.getCorrectionsForFragment('srcA', 1);
    expect(corrections).toHaveLength(1);
    expect(corrections[0].originalContent).toBe('helo world');

    const rev = session.getRevision(2);
    expect(rev).not.toBeNull();
    expect(rev!.changeType).toBe('correction-applied');
    expect(rev!.correctionId).toBe(corrections[0].correctionId);
    expect(rev!.correction).not.toBeNull();
    expect(rev!.correction!.actor).toBe('reviewer-1');
    expect(rev!.correction!.reason).toBe('fix');
  });

  it('rejects submission with an expired lease', async () => {
    const session = setupFinalFragment();
    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
      ttlMs: 50,
    });
    await new Promise((r) => setTimeout(r, 80));
    expect(() =>
      session.submitCorrection({
        leaseId: lease.leaseId,
        correctedContent: 'hello world',
        reason: 'fix',
      }),
    ).toThrow(LeaseExpiredError);
  });

  it('rejects submission with a consumed lease', () => {
    const session = setupFinalFragment();
    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
    });
    session.submitCorrection({
      leaseId: lease.leaseId,
      correctedContent: 'hello world',
      reason: 'fix',
    });
    expect(() =>
      session.submitCorrection({
        leaseId: lease.leaseId,
        correctedContent: 'hello world!',
        reason: 'fix again',
      }),
    ).toThrow(LeaseConsumedError);
  });

  it('rejects submission with a released lease', () => {
    const session = setupFinalFragment();
    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
    });
    const released = session.releaseLease(lease.leaseId);
    expect(released).toBe(true);
    expect(() =>
      session.submitCorrection({
        leaseId: lease.leaseId,
        correctedContent: 'hello world',
        reason: 'fix',
      }),
    ).toThrow(LeaseNotFoundError);
  });

  it('rejects submission with a non-existent lease id', () => {
    const session = setupFinalFragment();
    expect(() =>
      session.submitCorrection({
        leaseId: 'nonexistent-lease-id',
        correctedContent: 'hello world',
        reason: 'fix',
      }),
    ).toThrow(LeaseNotFoundError);
  });

  it('rejects submission when fragment content changed since lease (stale base)', () => {
    const session = store.session('stale');
    session.ingest(makeEvent('srcA', 1, 'partial', 'v1', 'e1'));
    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
    });
    session.ingest(makeEvent('srcA', 1, 'partial', 'v2-updated', 'e2'));

    expect(() =>
      session.submitCorrection({
        leaseId: lease.leaseId,
        correctedContent: 'corrected',
        reason: 'fix',
      }),
    ).toThrow(StaleBaseRevisionError);
  });

  it('builds supersedes lineage across multiple corrections', () => {
    const session = setupFinalFragment('lineage', 'v0');

    const lease1 = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
    });
    const c1 = session.submitCorrection({
      leaseId: lease1.leaseId,
      correctedContent: 'v1',
      reason: 'first fix',
    });
    expect(c1.supersedesCorrectionId).toBeNull();

    const lease2 = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-2',
      baseRevision: c1.revision,
    });
    const c2 = session.submitCorrection({
      leaseId: lease2.leaseId,
      correctedContent: 'v2',
      reason: 'second fix',
    });
    expect(c2.supersedesCorrectionId).toBe(c1.correctionId);

    const lease3 = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-3',
      baseRevision: c2.revision,
    });
    const c3 = session.submitCorrection({
      leaseId: lease3.leaseId,
      correctedContent: 'v3',
      reason: 'third fix',
    });
    expect(c3.supersedesCorrectionId).toBe(c2.correctionId);

    const all = session.getCorrectionsForFragment('srcA', 1);
    expect(all).toHaveLength(3);
    expect(all.map((c) => c.correctionId)).toEqual([
      c1.correctionId,
      c2.correctionId,
      c3.correctionId,
    ]);
    expect(session.getSnapshot().text).toBe('v3');
  });

  it('corrected fragment rejects incoming partial', () => {
    const session = setupFinalFragment();
    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
    });
    session.submitCorrection({
      leaseId: lease.leaseId,
      correctedContent: 'hello world',
      reason: 'fix',
    });

    const result = session.ingest(
      makeEvent('srcA', 1, 'partial', 'late partial', 'e2'),
    );
    expect(result.status).toBe('ignored');
    expect(result.reason).toContain('human-corrected');
    expect(session.getSnapshot().text).toBe('hello world');
  });

  it('corrected fragment rejects incoming final with different eventId', () => {
    const session = setupFinalFragment();
    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
    });
    session.submitCorrection({
      leaseId: lease.leaseId,
      correctedContent: 'hello world',
      reason: 'fix',
    });

    expect(() =>
      session.ingest(makeEvent('srcA', 1, 'final', 'new final', 'e2')),
    ).toThrow(SlotFinalizedError);
  });

  it('correction revision flows through existing consumer stream', () => {
    const session = setupFinalFragment('flow');
    const consumer = store.consumer('flow', 'qc-1');

    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
    });
    session.submitCorrection({
      leaseId: lease.leaseId,
      correctedContent: 'hello world',
      reason: 'typo',
    });

    const batch = consumer.read(100);
    expect(batch).toHaveLength(2);
    expect(batch[0].changeType).toBe('final-committed');
    expect(batch[1].changeType).toBe('correction-applied');
    expect(batch[1].snapshotText).toBe('hello world');
    expect(batch[1].correction).not.toBeNull();
    expect(batch[1].correction!.reason).toBe('typo');
    consumer.ack(batch[batch.length - 1].revision);
    expect(consumer.read(100)).toEqual([]);
  });

  it('releaseLease returns false for unknown lease', () => {
    const session = store.session('rl');
    expect(session.releaseLease('nope')).toBe(false);
  });

  it('getLease returns current lease state', () => {
    const session = setupFinalFragment('gl');
    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 1,
    });
    const got = session.getLease('srcA', 1);
    expect(got).not.toBeNull();
    expect(got!.leaseId).toBe(lease.leaseId);
    expect(got!.status).toBe('active');
  });

  it('revision numbers are strictly increasing across events and corrections', () => {
    const session = store.session('strict');
    session.ingest(makeEvent('srcA', 1, 'partial', 'a', 'e1'));
    session.ingest(makeEvent('srcA', 1, 'final', 'ab', 'e2'));
    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'r',
      baseRevision: 2,
    });
    const c = session.submitCorrection({
      leaseId: lease.leaseId,
      correctedContent: 'abc',
      reason: 'fix',
    });
    expect(c.revision).toBe(3);
    expect(session.getLatestRevisionNumber()).toBe(3);
  });
});
