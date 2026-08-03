import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, test } from "node:test";
import {
  createRecognitionStore,
  RecognitionStore,
  ReviewConflictError,
} from "../src";
import type { RecognitionEventInput } from "../src";

const stores: RecognitionStore[] = [];
const paths: string[] = [];

function createStore(filename?: string): {
  store: RecognitionStore;
  filename: string;
} {
  const resolved = filename ?? join(tempPath(), "review.db");
  const store = createRecognitionStore({ filename: resolved, busyTimeout: 5000 });
  stores.push(store);
  return { store, filename: resolved };
}

function tempPath(): string {
  const dir = mkdtempSync(join(tmpdir(), "asr-revision-review-"));
  paths.push(dir);
  return dir;
}

afterEach(() => {
  while (stores.length > 0) {
    stores.pop()?.close();
  }
  for (const path of paths.splice(0)) {
    rmSync(path, { recursive: true, force: true });
  }
});

function seedFinal(store: RecognitionStore): number {
  const events: RecognitionEventInput[] = [
    {
      eventId: "f1",
      sourceId: "a",
      sourceSeq: 0,
      isFinal: true,
      text: "hello world",
    },
    {
      eventId: "f2",
      sourceId: "a",
      sourceSeq: 1,
      isFinal: true,
      text: " from ASR",
    },
  ];
  const outcomes = store.ingest("s", events) as Array<{
    status: string;
    revision: number;
  }>;
  return outcomes[outcomes.length - 1]!.revision;
}

test("reviewer claims lease and submits correction, creating revision in same stream", () => {
  const { store } = createStore();
  const baseRevision = seedFinal(store);
  const claim = store.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "reviewer-1",
    baseRevision,
    ttlMs: 30000,
  });

  assert.equal(claim.acquired, true);
  assert.equal(claim.lease.active, true);
  assert.equal(claim.lease.actor, "reviewer-1");

  const consumer = store.createConsumer("s", "qc");
  const before = consumer.fetch();
  assert.equal(before.length, baseRevision);

  const outcome = store.submitCorrection("s", {
    leaseId: claim.lease.leaseId,
    actor: "reviewer-1",
    reason: "typo",
    correctedText: "hello brave",
    baseRevision,
  });

  assert.equal(outcome.revision, baseRevision + 1);
  assert.equal(outcome.correction.sourceId, "a");
  assert.equal(outcome.correction.sourceSeq, 0);
  assert.equal(outcome.correction.originalText, "hello world");
  assert.equal(outcome.correction.correctedText, "hello brave");
  assert.equal(outcome.correction.reason, "typo");
  assert.equal(outcome.correction.actor, "reviewer-1");
  assert.equal(outcome.correction.supersedes, undefined);

  const snapshot = store.getSnapshot("s");
  assert.equal(snapshot.revision, baseRevision + 1);
  assert.equal(snapshot.finalText, "hello brave from ASR");
  assert.equal(snapshot.summary.correctedSegmentCount, 1);
  const segment = snapshot.segments[0]!;
  assert.equal(segment.text, "hello brave");
  assert.equal(segment.originalText, "hello world");
  assert.equal(segment.correction?.correctionId, outcome.correction.correctionId);

  consumer.acknowledge(baseRevision);
  const after = consumer.fetch();
  assert.equal(after.length, 1);
  assert.equal(after[0]!.changeType, "correction");
  assert.equal(after[0]!.revision, baseRevision + 1);
  assert.equal(after[0]!.correctionId, outcome.correction.correctionId);
  assert.equal(after[0]!.sourceSeq, 0);
  assert.equal(after[0]!.snapshot.finalText, "hello brave from ASR");

  consumer.acknowledge(after[0]!.revision);
  assert.deepEqual(consumer.fetch(), []);
});

test("competing claim on same segment only one succeeds", () => {
  const { store } = createStore();
  const baseRevision = seedFinal(store);

  const first = store.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "alice",
    baseRevision,
    ttlMs: 30000,
  });
  assert.equal(first.acquired, true);

  assert.throws(
    () =>
      store.claimLease("s", {
        sourceId: "a",
        sourceSeq: 0,
        actor: "bob",
        baseRevision,
        ttlMs: 30000,
      }),
    (error: unknown) =>
      error instanceof ReviewConflictError && error.reason === "lease-taken"
  );

  const sameActor = store.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "alice",
    baseRevision,
    ttlMs: 30000,
  });
  assert.equal(sameActor.acquired, false);
  assert.equal(sameActor.lease.leaseId, first.lease.leaseId);
});

test("submission with expired lease fails distinctly", () => {
  let now = 1000;
  const dir = tempPath();
  const filename = join(dir, "expiring.db");
  const store = createRecognitionStore({
    filename,
    busyTimeout: 5000,
    now: () => now,
  });
  stores.push(store);

  const baseRevision = seedFinal(store);
  const claim = store.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "alice",
    baseRevision,
    ttlMs: 100,
  });

  now = 1200;
  assert.equal(claim.lease.expiresAt, 1100);
  assert.throws(
    () =>
      store.submitCorrection("s", {
        leaseId: claim.lease.leaseId,
        actor: "alice",
        reason: "late",
        correctedText: "x",
        baseRevision,
      }),
    (error: unknown) =>
      error instanceof ReviewConflictError && error.reason === "lease-expired"
  );

  now = 1300;
  const reclaimed = store.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "bob",
    baseRevision,
    ttlMs: 100,
  });
  assert.equal(reclaimed.acquired, true);
  assert.equal(reclaimed.lease.actor, "bob");
});

test("submission based on old revision fails distinctly", () => {
  const { store } = createStore();
  const baseRevision = seedFinal(store);

  const claim = store.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "alice",
    baseRevision,
    ttlMs: 30000,
  });

  store.submitCorrection("s", {
    leaseId: claim.lease.leaseId,
    actor: "alice",
    reason: "first pass",
    correctedText: "hello there",
    baseRevision,
  });

  const claim2 = store.claimLease("s", {
    sourceId: "a",
    sourceSeq: 1,
    actor: "alice",
    baseRevision: baseRevision + 1,
    ttlMs: 30000,
  });

  let caught: ReviewConflictError | undefined;
  try {
    store.submitCorrection("s", {
      leaseId: claim2.lease.leaseId,
      actor: "alice",
      reason: "stale",
      correctedText: "should fail",
      baseRevision,
    });
  } catch (error) {
    caught = error as ReviewConflictError;
  }
  assert.ok(caught);
  assert.equal(caught!.reason, "base-revision-stale");
  assert.equal(caught!.expectedRevision, baseRevision);
  assert.equal(caught!.actualRevision, baseRevision + 1);
});

test("supersedes lineage is preserved and original events are not modified", () => {
  const { store } = createStore();
  const baseRevision = seedFinal(store);

  const claim1 = store.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "alice",
    baseRevision,
    ttlMs: 30000,
  });
  const first = store.submitCorrection("s", {
    leaseId: claim1.lease.leaseId,
    actor: "alice",
    reason: "v1",
    correctedText: "hello v1",
    baseRevision,
  });

  const claim2 = store.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "bob",
    baseRevision: first.revision,
    ttlMs: 30000,
  });
  const second = store.submitCorrection("s", {
    leaseId: claim2.lease.leaseId,
    actor: "bob",
    reason: "v2",
    correctedText: "hello v2",
    baseRevision: first.revision,
  });

  assert.equal(second.correction.supersedes, first.correction.correctionId);
  assert.equal(second.correction.originalText, "hello v1");

  const lineage = store.getCorrectionLineage("s", "a", 0);
  assert.equal(lineage.length, 2);
  assert.equal(lineage[0]!.correctionId, first.correction.correctionId);
  assert.equal(lineage[1]!.supersedes, first.correction.correctionId);

  const history = store.getRevision("s", first.revision);
  assert.equal(history.snapshot.finalText, "hello v1 from ASR");
  const current = store.getSnapshot("s");
  assert.equal(current.finalText, "hello v2 from ASR");
  assert.equal(current.segments[0]!.originalText, "hello world");
});

test("lease and corrections survive restart and consumer resumes correction stream", () => {
  const dir = tempPath();
  const filename = join(dir, "restart.db");
  const first = createRecognitionStore({ filename, busyTimeout: 5000 });
  stores.push(first);

  const baseRevision = (() => {
    const outcomes = first.ingest("s", {
      eventId: "f1",
      sourceId: "a",
      sourceSeq: 0,
      isFinal: true,
      text: "restart me",
    }) as { revision: number };
    return outcomes.revision;
  })();

  const claim = first.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "alice",
    baseRevision,
    ttlMs: 60000,
  });
  first.acknowledge("s", "qc", baseRevision);
  first.close();
  stores.pop();

  const reopened = createRecognitionStore({ filename, busyTimeout: 5000 });
  stores.push(reopened);

  const lease = reopened.getLease("s", claim.lease.leaseId);
  assert.ok(lease);
  assert.equal(lease!.actor, "alice");
  assert.equal(lease!.active, true);

  const outcome = reopened.submitCorrection("s", {
    leaseId: claim.lease.leaseId,
    actor: "alice",
    reason: "post-restart",
    correctedText: "restarted",
    baseRevision,
  });

  const changes = reopened.fetchChanges("s", "qc");
  assert.equal(changes.length, 1);
  assert.equal(changes[0]!.revision, outcome.revision);
  assert.equal(changes[0]!.changeType, "correction");
  assert.equal(changes[0]!.snapshot.finalText, "restarted");
  assert.equal(reopened.getCorrectionLineage("s", "a", 0).length, 1);
});

test("release allows another actor to claim; wrong actor cannot release", () => {
  const { store } = createStore();
  const baseRevision = seedFinal(store);

  const claim = store.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "alice",
    baseRevision,
    ttlMs: 30000,
  });

  assert.throws(
    () => store.releaseLease("s", claim.lease.leaseId, "bob"),
    (error: unknown) =>
      error instanceof ReviewConflictError &&
      error.reason === "lease-actor-mismatch"
  );

  store.releaseLease("s", claim.lease.leaseId, "alice");
  assert.equal(store.getActiveLease("s", "a", 0), undefined);

  const next = store.claimLease("s", {
    sourceId: "a",
    sourceSeq: 0,
    actor: "bob",
    baseRevision,
    ttlMs: 30000,
  });
  assert.equal(next.acquired, true);
  assert.equal(next.lease.actor, "bob");
});
