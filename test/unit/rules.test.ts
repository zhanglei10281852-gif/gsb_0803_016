import { test } from "node:test";
import assert from "node:assert/strict";
import { RevisionHub, ConflictError, ValidationError } from "../../src/index";
import { tempDbPath } from "../helpers";

function freshHub() {
  const { path, cleanup } = tempDbPath();
  const hub = RevisionHub.open({ path });
  return { hub, cleanup: () => (hub.close(), cleanup()) };
}

test("exact re-delivery is idempotent (no new revision)", () => {
  const { hub, cleanup } = freshHub();
  try {
    const ev = {
      sessionId: "s",
      sourceId: "a",
      segmentId: "g1",
      eventId: "e1",
      sourceSeq: 1,
      kind: "partial" as const,
      text: "hello",
    };
    const r1 = hub.apply(ev);
    assert.equal(r1.outcome, "created");
    assert.equal(r1.effective, true);
    assert.equal(r1.revision, 1);

    const r2 = hub.apply({ ...ev });
    assert.equal(r2.outcome, "duplicate");
    assert.equal(r2.effective, false);
    assert.equal(r2.revision, null);

    assert.equal(hub.headRevision("s"), 1);
    assert.equal(hub.getSnapshot("s").segments.length, 1);
  } finally {
    cleanup();
  }
});

test("same eventId with different content is rejected as conflict", () => {
  const { hub, cleanup } = freshHub();
  try {
    hub.apply({
      sessionId: "s",
      sourceId: "a",
      segmentId: "g1",
      eventId: "e1",
      sourceSeq: 1,
      kind: "partial",
      text: "hello",
    });
    assert.throws(
      () =>
        hub.apply({
          sessionId: "s",
          sourceId: "a",
          segmentId: "g1",
          eventId: "e1",
          sourceSeq: 1,
          kind: "partial",
          text: "HELLO DIFFERENT",
        }),
      (e: unknown) => e instanceof ConflictError && e.code === "CONFLICT",
    );
    // Head revision unchanged; conflict left no partial write.
    assert.equal(hub.headRevision("s"), 1);
    assert.equal(hub.getSnapshot("s").segments[0]!.text, "hello");
  } finally {
    cleanup();
  }
});

test("older partial cannot overwrite a newer state", () => {
  const { hub, cleanup } = freshHub();
  try {
    const base = { sessionId: "s", sourceId: "a", segmentId: "g1", kind: "partial" as const };
    hub.apply({ ...base, eventId: "p1", sourceSeq: 1, text: "one" });
    hub.apply({ ...base, eventId: "p2", sourceSeq: 2, text: "one two" });
    // A late-arriving older partial (seq 1 already surpassed).
    const late = hub.apply({ ...base, eventId: "p1b", sourceSeq: 1, text: "stale" });
    assert.equal(late.outcome, "superseded");
    assert.equal(late.effective, false);
    assert.equal(hub.getSnapshot("s").segments[0]!.text, "one two");
  } finally {
    cleanup();
  }
});

test("final cannot be rolled back by a later partial", () => {
  const { hub, cleanup } = freshHub();
  try {
    const base = { sessionId: "s", sourceId: "a", segmentId: "g1" };
    hub.apply({ ...base, eventId: "p1", sourceSeq: 1, kind: "partial", text: "draft" });
    const fin = hub.apply({ ...base, eventId: "f1", sourceSeq: 2, kind: "final", text: "DONE" });
    assert.equal(fin.outcome, "updated");
    // A partial with an even higher seq must NOT override the final.
    const latePartial = hub.apply({
      ...base,
      eventId: "p9",
      sourceSeq: 9,
      kind: "partial",
      text: "late partial",
    });
    assert.equal(latePartial.outcome, "superseded");
    assert.equal(latePartial.effective, false);
    const seg = hub.getSnapshot("s").segments[0]!;
    assert.equal(seg.kind, "final");
    assert.equal(seg.text, "DONE");
  } finally {
    cleanup();
  }
});

test("revisions are strictly increasing per session across sources", () => {
  const { hub, cleanup } = freshHub();
  try {
    const seen: number[] = [];
    for (let i = 0; i < 5; i++) {
      const r = hub.apply({
        sessionId: "s",
        sourceId: `src-${i % 2}`,
        segmentId: `g-${i}`,
        eventId: `e-${i}`,
        sourceSeq: 1,
        kind: "partial",
        text: `t${i}`,
      });
      assert.equal(r.effective, true);
      seen.push(r.revision!);
    }
    for (let i = 1; i < seen.length; i++) {
      assert.ok(seen[i]! > seen[i - 1]!, "revisions strictly increase");
    }
    assert.deepEqual(seen, [1, 2, 3, 4, 5]);
  } finally {
    cleanup();
  }
});

test("validation rejects malformed events", () => {
  const { hub, cleanup } = freshHub();
  try {
    assert.throws(
      () =>
        hub.apply({
          sessionId: "",
          sourceId: "a",
          segmentId: "g",
          eventId: "e",
          sourceSeq: 1,
          kind: "partial",
          text: "x",
        }),
      (e: unknown) => e instanceof ValidationError,
    );
    assert.throws(
      () =>
        hub.apply({
          sessionId: "s",
          sourceId: "a",
          segmentId: "g",
          eventId: "e",
          sourceSeq: -1,
          kind: "partial",
          text: "x",
        }),
      (e: unknown) => e instanceof ValidationError,
    );
  } finally {
    cleanup();
  }
});

test("snapshot summary reflects finals, partials and full text ordering", () => {
  const { hub, cleanup } = freshHub();
  try {
    // Two sources, deterministic ordering by startMs.
    hub.apply({ sessionId: "s", sourceId: "b", segmentId: "g1", eventId: "b1", sourceSeq: 1, kind: "final", text: "world", startMs: 200 });
    hub.apply({ sessionId: "s", sourceId: "a", segmentId: "g1", eventId: "a1", sourceSeq: 1, kind: "final", text: "hello", startMs: 100 });
    const snap = hub.getSnapshot("s");
    assert.equal(snap.summary.segmentCount, 2);
    assert.equal(snap.summary.finalCount, 2);
    assert.equal(snap.summary.fullText, "hello world");
    assert.deepEqual(Object.keys(snap.summary.perSource), ["a", "b"]);
  } finally {
    cleanup();
  }
});
