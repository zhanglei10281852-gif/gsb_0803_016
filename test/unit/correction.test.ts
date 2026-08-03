import { test } from "node:test";
import assert from "node:assert/strict";
import {
  RevisionHub,
  LeaseConflictError,
  LeaseExpiredError,
  NoLeaseError,
  StaleBaseError,
  ValidationError,
} from "../../src/index";
import { tempDbPath } from "../helpers";

/** A hub whose clock we control, so lease TTLs expire deterministically. */
function controllableHub() {
  const { path, cleanup } = tempDbPath();
  let now = 1_000_000;
  const hub = RevisionHub.open({ path, clock: () => now });
  return {
    hub,
    advance: (ms: number) => {
      now += ms;
    },
    setNow: (v: number) => {
      now = v;
    },
    cleanup: () => (hub.close(), cleanup()),
  };
}

/** Seed one recognised, finalised segment; return its current revision. */
function seedSegment(hub: RevisionHub, sessionId = "s", sourceId = "a", segmentId = "g1") {
  hub.apply({ sessionId, sourceId, segmentId, eventId: "p1", sourceSeq: 1, kind: "partial", text: "helo world" });
  hub.apply({ sessionId, sourceId, segmentId, eventId: "f1", sourceSeq: 2, kind: "final", text: "helo world" });
  const snap = hub.getSnapshot(sessionId);
  return snap.segments[0]!.revision;
}

test("a correction is a new revision in the same stream, read by existing consumers", () => {
  const { hub, cleanup } = controllableHub();
  try {
    const base = seedSegment(hub);

    // Existing consumer reads recognition revisions and acks them.
    const before = hub.pull("s", "qc");
    assert.equal(before.every((r) => r.origin === "recognition"), true);
    hub.ack("s", "qc", before[before.length - 1]!.revision);

    // Reviewer leases, corrects.
    const lease = hub.acquireLease({
      sessionId: "s",
      sourceId: "a",
      segmentId: "g1",
      actor: "reviewer-1",
      baseRevision: base,
      ttlMs: 60_000,
    });
    const res = hub.submitCorrection({
      leaseId: lease.leaseId,
      actor: "reviewer-1",
      text: "hello world",
      reason: "spelling",
    });
    assert.equal(res.applied, true);
    assert.equal(res.supersedesRevision, base);
    assert.ok(res.revision > base);

    // The SAME consumer, continuing from its cursor, now sees the correction.
    const after = hub.pull("s", "qc");
    assert.equal(after.length, 1);
    const rec = after[0]!;
    assert.equal(rec.revision, res.revision);
    assert.equal(rec.origin, "correction");
    assert.equal(rec.actor, "reviewer-1");
    assert.equal(rec.reason, "spelling");
    assert.equal(rec.supersedesRevision, base);
    assert.equal(rec.correctionId, res.correctionId);
    assert.equal(rec.text, "hello world");

    // Snapshot reflects corrected text + provenance.
    const seg = hub.getSnapshot("s").segments[0]!;
    assert.equal(seg.text, "hello world");
    assert.equal(seg.origin, "correction");
    assert.equal(seg.actor, "reviewer-1");
  } finally {
    cleanup();
  }
});

test("corrections do not rewrite the raw recognition events", () => {
  const { hub, cleanup } = controllableHub();
  try {
    const base = seedSegment(hub);
    const lease = hub.acquireLease({ sessionId: "s", sourceId: "a", segmentId: "g1", actor: "r", baseRevision: base, ttlMs: 60_000 });
    hub.submitCorrection({ leaseId: lease.leaseId, actor: "r", text: "corrected", reason: "fix" });

    // Raw events table untouched: still exactly the two source events, verbatim.
    const events = hub.database
      .prepare("SELECT event_id, text FROM events WHERE session_id='s' ORDER BY source_seq")
      .all() as Array<{ event_id: string; text: string }>;
    assert.deepEqual(events.map((e) => e.event_id), ["p1", "f1"]);
    assert.equal(events.every((e) => e.text === "helo world"), true);

    // Lineage is recorded separately in corrections.
    const corr = hub.database
      .prepare("SELECT actor, reason, supersedes_revision FROM corrections WHERE session_id='s'")
      .get() as { actor: string; reason: string; supersedes_revision: number };
    assert.equal(corr.actor, "r");
    assert.equal(corr.reason, "fix");
    assert.equal(corr.supersedes_revision, base);
  } finally {
    cleanup();
  }
});

test("competing lease acquisition: only one wins while a lease is live", () => {
  const { hub, cleanup } = controllableHub();
  try {
    const base = seedSegment(hub);
    const ref = { sessionId: "s", sourceId: "a", segmentId: "g1", baseRevision: base, ttlMs: 60_000 };
    hub.acquireLease({ ...ref, actor: "r1" });
    assert.throws(
      () => hub.acquireLease({ ...ref, actor: "r2" }),
      (e: unknown) => e instanceof LeaseConflictError && e.code === "LEASE_CONFLICT",
    );
  } finally {
    cleanup();
  }
});

test("an expired lease can be taken over by another reviewer", () => {
  const { hub, advance, cleanup } = controllableHub();
  try {
    const base = seedSegment(hub);
    const ref = { sessionId: "s", sourceId: "a", segmentId: "g1", baseRevision: base };
    hub.acquireLease({ ...ref, actor: "r1", ttlMs: 1_000 });
    advance(1_500); // lease TTL elapses
    const l2 = hub.acquireLease({ ...ref, actor: "r2", ttlMs: 60_000 });
    assert.equal(l2.actor, "r2");
    // r2 can now submit successfully.
    const res = hub.submitCorrection({ leaseId: l2.leaseId, actor: "r2", text: "ok", reason: "late fix" });
    assert.equal(res.applied, true);
  } finally {
    cleanup();
  }
});

test("submitting after TTL expiry fails with LeaseExpiredError", () => {
  const { hub, advance, cleanup } = controllableHub();
  try {
    const base = seedSegment(hub);
    const lease = hub.acquireLease({ sessionId: "s", sourceId: "a", segmentId: "g1", actor: "r", baseRevision: base, ttlMs: 1_000 });
    advance(2_000);
    assert.throws(
      () => hub.submitCorrection({ leaseId: lease.leaseId, actor: "r", text: "x", reason: "y" }),
      (e: unknown) => e instanceof LeaseExpiredError && e.code === "LEASE_EXPIRED",
    );
    // Nothing changed.
    assert.equal(hub.getSnapshot("s").segments[0]!.origin, "recognition");
  } finally {
    cleanup();
  }
});

test("submitting against a stale base fails with StaleBaseError", () => {
  const { hub, cleanup } = controllableHub();
  try {
    const base = seedSegment(hub);
    // Reviewer leases at base...
    const lease = hub.acquireLease({ sessionId: "s", sourceId: "a", segmentId: "g1", actor: "r", baseRevision: base, ttlMs: 60_000 });
    // ...but the recognizer emits a newer final before the reviewer submits.
    hub.apply({ sessionId: "s", sourceId: "a", segmentId: "g1", eventId: "f2", sourceSeq: 3, kind: "final", text: "newer recog" });
    assert.throws(
      () => hub.submitCorrection({ leaseId: lease.leaseId, actor: "r", text: "x", reason: "y" }),
      (e: unknown) => e instanceof StaleBaseError && e.code === "STALE_BASE",
    );
  } finally {
    cleanup();
  }
});

test("a lease baseRevision that never matched (wrong base) is a stale conflict", () => {
  const { hub, cleanup } = controllableHub();
  try {
    const base = seedSegment(hub);
    // Acquire with a bogus base one ahead of reality.
    const lease = hub.acquireLease({ sessionId: "s", sourceId: "a", segmentId: "g1", actor: "r", baseRevision: base + 5, ttlMs: 60_000 });
    assert.throws(
      () => hub.submitCorrection({ leaseId: lease.leaseId, actor: "r", text: "x", reason: "y" }),
      (e: unknown) => e instanceof StaleBaseError,
    );
  } finally {
    cleanup();
  }
});

test("unknown lease / wrong actor fails with NoLeaseError", () => {
  const { hub, cleanup } = controllableHub();
  try {
    const base = seedSegment(hub);
    const lease = hub.acquireLease({ sessionId: "s", sourceId: "a", segmentId: "g1", actor: "r", baseRevision: base, ttlMs: 60_000 });
    assert.throws(
      () => hub.submitCorrection({ leaseId: "does-not-exist", actor: "r", text: "x", reason: "y" }),
      (e: unknown) => e instanceof NoLeaseError && e.code === "NO_LEASE",
    );
    // Correct lease, wrong actor.
    assert.throws(
      () => hub.submitCorrection({ leaseId: lease.leaseId, actor: "intruder", text: "x", reason: "y" }),
      (e: unknown) => e instanceof NoLeaseError,
    );
  } finally {
    cleanup();
  }
});

test("recognition after a correction cannot roll it back", () => {
  const { hub, cleanup } = controllableHub();
  try {
    const base = seedSegment(hub);
    const lease = hub.acquireLease({ sessionId: "s", sourceId: "a", segmentId: "g1", actor: "r", baseRevision: base, ttlMs: 60_000 });
    const res = hub.submitCorrection({ leaseId: lease.leaseId, actor: "r", text: "authoritative", reason: "fix" });

    // A late recognition event (even a higher-seq final) must be superseded.
    const late = hub.apply({ sessionId: "s", sourceId: "a", segmentId: "g1", eventId: "f9", sourceSeq: 99, kind: "final", text: "late recog" });
    assert.equal(late.outcome, "superseded");
    assert.equal(late.effective, false);

    const seg = hub.getSnapshot("s").segments[0]!;
    assert.equal(seg.text, "authoritative");
    assert.equal(seg.origin, "correction");
    assert.equal(seg.revision, res.revision);
  } finally {
    cleanup();
  }
});

test("submitCorrection is idempotent on correctionId", () => {
  const { hub, cleanup } = controllableHub();
  try {
    const base = seedSegment(hub);
    const lease = hub.acquireLease({ sessionId: "s", sourceId: "a", segmentId: "g1", actor: "r", baseRevision: base, ttlMs: 60_000 });
    const first = hub.submitCorrection({ leaseId: lease.leaseId, actor: "r", text: "fixed", reason: "typo", correctionId: "corr-1" });
    assert.equal(first.applied, true);

    // Re-submit same correctionId (e.g. retry after crash) → no new revision.
    const headBefore = hub.headRevision("s");
    const again = hub.submitCorrection({ leaseId: lease.leaseId, actor: "r", text: "fixed", reason: "typo", correctionId: "corr-1" });
    assert.equal(again.applied, false);
    assert.equal(again.revision, first.revision);
    assert.equal(hub.headRevision("s"), headBefore, "no extra revision on idempotent replay");
  } finally {
    cleanup();
  }
});

test("supersedes lineage chains across successive corrections", () => {
  const { hub, cleanup } = controllableHub();
  try {
    const base = seedSegment(hub);
    const l1 = hub.acquireLease({ sessionId: "s", sourceId: "a", segmentId: "g1", actor: "r1", baseRevision: base, ttlMs: 60_000 });
    const c1 = hub.submitCorrection({ leaseId: l1.leaseId, actor: "r1", text: "v2", reason: "first" });

    // Second reviewer bases on the corrected revision.
    const l2 = hub.acquireLease({ sessionId: "s", sourceId: "a", segmentId: "g1", actor: "r2", baseRevision: c1.revision, ttlMs: 60_000 });
    const c2 = hub.submitCorrection({ leaseId: l2.leaseId, actor: "r2", text: "v3", reason: "second" });

    assert.equal(c1.supersedesRevision, base);
    assert.equal(c2.supersedesRevision, c1.revision);
    assert.equal(hub.getSnapshot("s").segments[0]!.text, "v3");
  } finally {
    cleanup();
  }
});

test("validation: bad ttl / missing reason are rejected", () => {
  const { hub, cleanup } = controllableHub();
  try {
    const base = seedSegment(hub);
    assert.throws(
      () => hub.acquireLease({ sessionId: "s", sourceId: "a", segmentId: "g1", actor: "r", baseRevision: base, ttlMs: 0 }),
      (e: unknown) => e instanceof ValidationError,
    );
    const lease = hub.acquireLease({ sessionId: "s", sourceId: "a", segmentId: "g1", actor: "r", baseRevision: base, ttlMs: 1000 });
    assert.throws(
      () => hub.submitCorrection({ leaseId: lease.leaseId, actor: "r", text: "x", reason: "" }),
      (e: unknown) => e instanceof ValidationError,
    );
  } finally {
    cleanup();
  }
});
