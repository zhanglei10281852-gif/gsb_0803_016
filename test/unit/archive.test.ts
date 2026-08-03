import { test } from "node:test";
import assert from "node:assert/strict";
import {
  RevisionHub,
  ArchiveIntegrityError,
  ArchiveConflictError,
  type Snapshot,
} from "../../src/index";
import { tempDbPath } from "../helpers";

/** Build a source hub with recognition + a human correction + a consumer cursor. */
function buildSource() {
  const { path, cleanup } = tempDbPath();
  let now = 5_000_000;
  const hub = RevisionHub.open({ path, clock: () => now });
  const sessionId = "handoff";

  // Two sources, a few segments, partials + finals.
  hub.apply({ sessionId, sourceId: "a", segmentId: "g1", eventId: "a-p1", sourceSeq: 1, kind: "partial", text: "helo", startMs: 0, endMs: 100 });
  hub.apply({ sessionId, sourceId: "a", segmentId: "g1", eventId: "a-f1", sourceSeq: 2, kind: "final", text: "helo world", startMs: 0, endMs: 200 });
  hub.apply({ sessionId, sourceId: "b", segmentId: "g1", eventId: "b-f1", sourceSeq: 1, kind: "final", text: "second source", startMs: 300, endMs: 500 });

  // A correction carrying actor / reason / supersedes lineage.
  const seg = hub.getSnapshot(sessionId).segments.find((s) => s.sourceId === "a")!;
  const lease = hub.acquireLease({ sessionId, sourceId: "a", segmentId: "g1", actor: "reviewer-9", baseRevision: seg.revision, ttlMs: 60_000 });
  hub.submitCorrection({ leaseId: lease.leaseId, actor: "reviewer-9", text: "hello world", reason: "spelling fix", correctionId: "corr-A" });

  // A consumer that has read + acked part of the stream.
  const batch = hub.pull(sessionId, "qc", { limit: 2 });
  hub.ack(sessionId, "qc", batch[batch.length - 1]!.revision);

  return { hub, sessionId, cleanup, setNow: (v: number) => (now = v) };
}

function fingerprint(hub: RevisionHub, sessionId: string): string {
  const snap: Snapshot = hub.getSnapshot(sessionId);
  const revs = hub.pull(sessionId, "fp-reader", { afterRevision: 0, limit: 10_000 });
  return JSON.stringify({
    head: snap.headRevision,
    segments: snap.segments,
    summary: snap.summary,
    revisions: revs,
  });
}

test("export→import into an empty DB is equivalent (snapshot, history, cursor)", () => {
  const src = buildSource();
  try {
    const archive = src.hub.exportSessionToString(src.sessionId);
    const srcFp = fingerprint(src.hub, src.sessionId);
    const srcCursor = src.hub.getCursor(src.sessionId, "qc");

    const dst = tempDbPath();
    const dstHub = RevisionHub.open({ path: dst.path });
    try {
      const res = dstHub.importSession(archive);
      assert.equal(res.imported, true);
      assert.equal(res.sessionId, src.sessionId);

      // Snapshot + revision history identical.
      assert.equal(fingerprint(dstHub, src.sessionId), srcFp);

      // Resumable cursor carried: qc resumes exactly where it left off.
      assert.equal(dstHub.getCursor(src.sessionId, "qc"), srcCursor);
      const srcNext = src.hub.pull(src.sessionId, "qc").map((r) => r.revision);
      const dstNext = dstHub.pull(src.sessionId, "qc").map((r) => r.revision);
      assert.deepEqual(dstNext, srcNext);

      // Correction lineage carried verbatim in the stream.
      const corr = dstHub.pull(src.sessionId, "lineage", { afterRevision: 0, limit: 999 })
        .find((r) => r.origin === "correction")!;
      assert.equal(corr.actor, "reviewer-9");
      assert.equal(corr.reason, "spelling fix");
      assert.equal(corr.correctionId, "corr-A");
      assert.ok(corr.supersedesRevision !== null);
    } finally {
      dstHub.close();
      dst.cleanup();
    }
  } finally {
    src.hub.close();
    src.cleanup();
  }
});

test("re-importing the identical archive is idempotent", () => {
  const src = buildSource();
  try {
    const archive = src.hub.exportSessionToString(src.sessionId);
    const dst = tempDbPath();
    const dstHub = RevisionHub.open({ path: dst.path });
    try {
      const first = dstHub.importSession(archive);
      assert.equal(first.imported, true);
      const fp1 = fingerprint(dstHub, src.sessionId);

      const second = dstHub.importSession(archive);
      assert.equal(second.imported, false, "second import must be a no-op");
      assert.equal(second.digest, first.digest);
      // State unchanged.
      assert.equal(fingerprint(dstHub, src.sessionId), fp1);
    } finally {
      dstHub.close();
      dst.cleanup();
    }
  } finally {
    src.hub.close();
    src.cleanup();
  }
});

test("export is deterministic: two exports of the same state share a digest", () => {
  const src = buildSource();
  try {
    // Different header timestamps must NOT change the content digest.
    src.setNow(9_000_000);
    const a1 = src.hub.exportSessionToString(src.sessionId);
    src.setNow(9_999_999);
    const a2 = src.hub.exportSessionToString(src.sessionId);
    assert.notEqual(a1, a2, "headers differ (createdAt), so raw text differs");

    const dst1 = tempDbPath();
    const dst2 = tempDbPath();
    const h1 = RevisionHub.open({ path: dst1.path });
    const h2 = RevisionHub.open({ path: dst2.path });
    try {
      const r1 = h1.importSession(a1);
      const r2 = h2.importSession(a2);
      assert.equal(r1.digest, r2.digest, "content digest is stable across header time");
    } finally {
      h1.close(); h2.close(); dst1.cleanup(); dst2.cleanup();
    }
  } finally {
    src.hub.close();
    src.cleanup();
  }
});

test("a different archive for an occupied session is a conflict", () => {
  const src = buildSource();
  try {
    const archive = src.hub.exportSessionToString(src.sessionId);
    const dst = tempDbPath();
    const dstHub = RevisionHub.open({ path: dst.path });
    try {
      dstHub.importSession(archive);
      // Tamper with a text field to make a genuinely different (but internally
      // consistent) archive... actually simplest: build a divergent source.
      const other = buildSource();
      // Force the other archive to share the same sessionId but differ.
      other.hub.apply({ sessionId: src.sessionId, sourceId: "c", segmentId: "gX", eventId: "c1", sourceSeq: 1, kind: "final", text: "divergent" });
      const otherArchive = other.hub.exportSessionToString(src.sessionId);
      try {
        assert.throws(
          () => dstHub.importSession(otherArchive),
          (e: unknown) => e instanceof ArchiveConflictError && e.code === "ARCHIVE_CONFLICT",
        );
      } finally {
        other.hub.close();
        other.cleanup();
      }
    } finally {
      dstHub.close();
      dst.cleanup();
    }
  } finally {
    src.hub.close();
    src.cleanup();
  }
});

test("importing into a session that already has live content refuses", () => {
  const src = buildSource();
  try {
    const archive = src.hub.exportSessionToString(src.sessionId);
    const dst = tempDbPath();
    const dstHub = RevisionHub.open({ path: dst.path });
    try {
      // Populate the same sessionId via normal recognition first.
      dstHub.apply({ sessionId: src.sessionId, sourceId: "z", segmentId: "gz", eventId: "z1", sourceSeq: 1, kind: "final", text: "live" });
      assert.throws(
        () => dstHub.importSession(archive),
        (e: unknown) => e instanceof ArchiveConflictError,
      );
    } finally {
      dstHub.close();
      dst.cleanup();
    }
  } finally {
    src.hub.close();
    src.cleanup();
  }
});

/** Import into a fresh DB and expect an integrity failure with no visible state. */
function expectRejectedWithNoState(mutate: (lines: string[]) => string[], sessionId: string) {
  const src = buildSource();
  try {
    const archive = src.hub.exportSessionToString(sessionId);
    const lines = archive.split("\n").filter((l) => l.length > 0);
    const mutated = mutate(lines).join("\n") + "\n";

    const dst = tempDbPath();
    const dstHub = RevisionHub.open({ path: dst.path });
    try {
      assert.throws(
        () => dstHub.importSession(mutated),
        (e: unknown) => e instanceof ArchiveIntegrityError && e.code === "ARCHIVE_INTEGRITY",
      );
      // Nothing became visible: session is empty.
      assert.equal(dstHub.headRevision(sessionId), 0);
      assert.equal(dstHub.getSnapshot(sessionId).segments.length, 0);
      assert.equal(dstHub.pull(sessionId, "probe", { afterRevision: 0 }).length, 0);
    } finally {
      dstHub.close();
      dst.cleanup();
    }
  } finally {
    src.hub.close();
    src.cleanup();
  }
}

test("reordered records fail before any state is visible", () => {
  expectRejectedWithNoState((lines) => {
    // Swap two body records (indices 2 and 3) — chain + content digests break.
    const copy = lines.slice();
    const tmp = copy[2]!;
    copy[2] = copy[3]!;
    copy[3] = tmp;
    return copy;
  }, "handoff");
});

test("truncated archive (missing trailer) fails before any state is visible", () => {
  expectRejectedWithNoState((lines) => lines.slice(0, lines.length - 1), "handoff");
});

test("truncated body (dropped middle record) fails before any state is visible", () => {
  expectRejectedWithNoState((lines) => {
    const copy = lines.slice();
    copy.splice(3, 1); // drop a body record but keep the trailer
    return copy;
  }, "handoff");
});

test("tampered field fails before any state is visible", () => {
  expectRejectedWithNoState((lines) => {
    return lines.map((l) => {
      const obj = JSON.parse(l);
      if (obj.type === "revision" && typeof obj.text === "string") {
        obj.text = obj.text + " (tampered)";
      }
      return JSON.stringify(obj);
    });
  }, "handoff");
});

test("wrong/unsupported format version is rejected", () => {
  expectRejectedWithNoState((lines) => {
    const header = JSON.parse(lines[0]!);
    header.format = 999;
    lines[0] = JSON.stringify(header);
    return lines;
  }, "handoff");
});

test("HMAC secret: archive verifies only with the matching secret", () => {
  const src = buildSource();
  try {
    const archive = src.hub.exportSessionToString(src.sessionId, { secret: "top-secret" });
    const dst = tempDbPath();
    const dstHub = RevisionHub.open({ path: dst.path });
    try {
      // Wrong / missing secret → integrity failure, no state.
      assert.throws(
        () => dstHub.importSession(archive, { secret: "wrong" }),
        (e: unknown) => e instanceof ArchiveIntegrityError,
      );
      assert.throws(
        () => dstHub.importSession(archive),
        (e: unknown) => e instanceof ArchiveIntegrityError,
      );
      assert.equal(dstHub.headRevision(src.sessionId), 0);
      // Correct secret → imports fine.
      const res = dstHub.importSession(archive, { secret: "top-secret" });
      assert.equal(res.imported, true);
      assert.ok(res.headRevision > 0);
    } finally {
      dstHub.close();
      dst.cleanup();
    }
  } finally {
    src.hub.close();
    src.cleanup();
  }
});

test("streamed import: arbitrary chunk boundaries are handled", () => {
  const src = buildSource();
  try {
    const archive = src.hub.exportSessionToString(src.sessionId);
    // Slice the archive into fixed-size chunks that cut across newlines.
    const chunks: string[] = [];
    for (let i = 0; i < archive.length; i += 7) chunks.push(archive.slice(i, i + 7));

    const dst = tempDbPath();
    const dstHub = RevisionHub.open({ path: dst.path });
    try {
      const res = dstHub.importSession(chunks);
      assert.equal(res.imported, true);
      assert.equal(fingerprint(dstHub, src.sessionId), fingerprint(src.hub, src.sessionId));
    } finally {
      dstHub.close();
      dst.cleanup();
    }
  } finally {
    src.hub.close();
    src.cleanup();
  }
});

test("forward-compat: unknown optional fields survive an import→export round-trip", () => {
  const src = buildSource();
  try {
    const archive = src.hub.exportSessionToString(src.sessionId);
    const lines = archive.split("\n").filter((l) => l.length > 0);

    // Simulate a newer exporter that added optional fields on header + a revision,
    // and recompute the digests so the (legitimate) archive still verifies.
    // We do this by re-deriving the trailer using the library's own exporter is
    // not possible here, so instead we inject fields and rebuild digests via a
    // second hub round-trip: import the ORIGINAL, then re-export adds nothing —
    // so we instead craft with matching digests using the exporter contract:
    // easiest correct approach: add ext fields, then fix trailer by re-hashing.
    const withExt = lines.map((l) => {
      const obj = JSON.parse(l);
      if (obj.type === "header") obj.futureHeaderFlag = "keep-me";
      if (obj.type === "revision" && obj.revision === 1) obj.futureField = { nested: true, n: 42 };
      return JSON.stringify(obj);
    });

    // Recompute trailer to make this a *valid* next-version archive.
    // (Body excludes header; chain includes header. Mirror the library.)
    const { createHash } = require("node:crypto") as typeof import("node:crypto");
    const canon = (s: string) => JSON.stringify(sortKeys(JSON.parse(s)));
    function sortKeys(v: any): any {
      if (Array.isArray(v)) return v.map(sortKeys);
      if (v && typeof v === "object") {
        const o: any = {};
        for (const k of Object.keys(v).sort()) o[k] = sortKeys(v[k]);
        return o;
      }
      return v;
    }
    const body = withExt.slice(1, withExt.length - 1).map(canon);
    const headerCanon = canon(withExt[0]!);
    // chain
    let prev = "";
    const fold = (line: string) => {
      const h = createHash("sha256");
      h.update(prev); h.update("\n"); h.update(line);
      prev = h.digest("hex");
    };
    fold(headerCanon);
    for (const b of body) fold(b);
    const chain = prev;
    const contentH = createHash("sha256");
    for (const b of body) { contentH.update(b); contentH.update("\n"); }
    const content = contentH.digest("hex");
    const trailer = JSON.stringify({ type: "trailer", count: body.length, content, chain });
    const rebuilt = [headerCanon, ...body, trailer].join("\n") + "\n";

    const dst = tempDbPath();
    const dstHub = RevisionHub.open({ path: dst.path });
    try {
      const res = dstHub.importSession(rebuilt);
      assert.equal(res.imported, true, "a valid next-version archive imports");

      // Re-export should carry the unknown fields back out.
      const reExported = dstHub.exportSessionToString(src.sessionId);
      const reLines = reExported.split("\n").filter((l) => l.length > 0).map((l) => JSON.parse(l));
      const header = reLines.find((r) => r.type === "header");
      assert.equal(header.futureHeaderFlag, "keep-me", "unknown header field preserved");
      const rev1 = reLines.find((r) => r.type === "revision" && r.revision === 1);
      assert.deepEqual(rev1.futureField, { nested: true, n: 42 }, "unknown revision field preserved");
    } finally {
      dstHub.close();
      dst.cleanup();
    }
  } finally {
    src.hub.close();
    src.cleanup();
  }
});
