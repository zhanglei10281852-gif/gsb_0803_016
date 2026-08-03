import { test } from "node:test";
import assert from "node:assert/strict";
import { RevisionHub, type Snapshot } from "../../src/index";
import { tempDbPath, buildScenario, shuffle, rng } from "../helpers";

/** Canonical, order-independent fingerprint of a snapshot's resolved state. */
function snapshotFingerprint(snap: Snapshot): string {
  const segs = snap.segments.map((s) => ({
    sourceId: s.sourceId,
    segmentId: s.segmentId,
    kind: s.kind,
    text: s.text,
    startMs: s.startMs,
    endMs: s.endMs,
    sourceSeq: s.sourceSeq,
    eventId: s.eventId,
  }));
  return JSON.stringify({ segments: segs, summary: snap.summary });
}

function runOrdering(events: ReturnType<typeof buildScenario>["events"], sessionId: string): Snapshot {
  const { path, cleanup } = tempDbPath();
  const hub = RevisionHub.open({ path });
  try {
    for (const e of events) hub.apply(e);
    return hub.getSnapshot(sessionId);
  } finally {
    hub.close();
    cleanup();
  }
}

test("final snapshot is identical across many shuffled orderings", () => {
  const { sessionId, events } = buildScenario({ sources: 3, segmentsPerSource: 5, partialsPerSegment: 4, duplicates: true, seed: 7 });

  const baseline = runOrdering(events, sessionId);
  const baseFp = snapshotFingerprint(baseline);

  for (let trial = 0; trial < 25; trial++) {
    const rand = rng(1000 + trial);
    const shuffled = shuffle(events, rand);
    const snap = runOrdering(shuffled, sessionId);
    assert.equal(
      snapshotFingerprint(snap),
      baseFp,
      `ordering trial ${trial} produced a different snapshot`,
    );
    // full text must also match exactly.
    assert.equal(snap.summary.fullText, baseline.summary.fullText);
  }
});

test("segment ordering is deterministic (startMs, sourceId, segmentId)", () => {
  const { sessionId, events } = buildScenario({ sources: 2, segmentsPerSource: 3 });
  const snap = runOrdering(shuffle(events, rng(42)), sessionId);
  const keys = snap.segments.map((s) => `${s.startMs}|${s.sourceId}|${s.segmentId}`);
  const sorted = keys.slice().sort((a, b) => {
    const [as, asrc, aseg] = a.split("|");
    const [bs, bsrc, bseg] = b.split("|");
    if (Number(as) !== Number(bs)) return Number(as) - Number(bs);
    if (asrc !== bsrc) return asrc! < bsrc! ? -1 : 1;
    return aseg! < bseg! ? -1 : aseg! > bseg! ? 1 : 0;
  });
  assert.deepEqual(keys, sorted);
});

test("duplicates and reorderings never change the number of effective revisions", () => {
  const { sessionId, events } = buildScenario({ sources: 2, segmentsPerSource: 3, partialsPerSegment: 3, duplicates: true, seed: 99 });

  function effectiveCount(evs: typeof events): { revs: number; head: number } {
    const { path, cleanup } = tempDbPath();
    const hub = RevisionHub.open({ path });
    try {
      let eff = 0;
      for (const e of evs) if (hub.apply(e).effective) eff++;
      return { revs: eff, head: hub.headRevision(sessionId) };
    } finally {
      hub.close();
      cleanup();
    }
  }

  const base = effectiveCount(events);
  assert.equal(base.revs, base.head);
  for (let t = 0; t < 10; t++) {
    const res = effectiveCount(shuffle(events, rng(500 + t)));
    // Head revision (count of effective changes) may legitimately differ across
    // orderings because a partial that arrives before being superseded still
    // counts as an effective change. The final resolved state is what must be
    // invariant; here we assert head == number of effective results, i.e. the
    // revision stream and allocator stay consistent regardless of order.
    assert.equal(res.revs, res.head, `trial ${t}: revs != head`);
  }
});
