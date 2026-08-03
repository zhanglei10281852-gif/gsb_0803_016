import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, test } from "node:test";
import { createRecognitionStore, EventConflictError, RecognitionStore } from "../src";
import type { RecognitionEventInput } from "../src";

const stores: RecognitionStore[] = [];
const paths: string[] = [];

function createStore(): { store: RecognitionStore; filename: string } {
  const dir = mkdtempSync(join(tmpdir(), "asr-revision-unit-"));
  const filename = join(dir, "store.db");
  paths.push(dir);
  const store = createRecognitionStore({ filename, busyTimeout: 5000 });
  stores.push(store);
  return { store, filename };
}

afterEach(() => {
  while (stores.length > 0) {
    stores.pop()?.close();
  }
  for (const path of paths.splice(0)) {
    rmSync(path, { recursive: true, force: true });
  }
});

function shuffle<T>(items: readonly T[], seed: number): T[] {
  const result = [...items];
  let value = seed;
  for (let i = result.length - 1; i > 0; i -= 1) {
    value = (value * 1103515245 + 12345) & 0x7fffffff;
    const j = value % (i + 1);
    [result[i], result[j]] = [result[j] as T, result[i] as T];
  }
  return result;
}

test("empty session returns revision zero snapshot", () => {
  const { store } = createStore();
  const snapshot = store.getSnapshot("session");
  assert.equal(snapshot.revision, 0);
  assert.deepEqual(snapshot.segments, []);
  assert.equal(snapshot.text, "");
  assert.equal(snapshot.summary.eventCount, 0);
});

test("partial creates revision and snapshot, duplicate is idempotent", () => {
  const { store } = createStore();
  const event: RecognitionEventInput = {
    eventId: "p1",
    sourceId: "a",
    sourceSeq: 1,
    isFinal: false,
    text: "hel",
  };

  const first = store.ingest("session", event);
  assert.deepEqual(first, {
    status: "inserted",
    eventId: "p1",
    revision: 1,
  });

  const snapshot = store.getSnapshot("session");
  assert.equal(snapshot.revision, 1);
  assert.equal(snapshot.text, "hel");
  assert.equal(snapshot.finalText, "");
  assert.equal(snapshot.summary.activePartialCount, 1);

  const duplicate = store.ingest("session", event);
  assert.deepEqual(duplicate, {
    status: "duplicate",
    eventId: "p1",
    revision: 1,
  });
  assert.equal(store.getSnapshot("session").revision, 1);
});

test("same event identifier with different content is rejected", () => {
  const { store } = createStore();
  store.ingest("session", {
    eventId: "p1",
    sourceId: "a",
    sourceSeq: 1,
    isFinal: false,
    text: "hel",
  });

  assert.throws(
    () =>
      store.ingest("session", {
        eventId: "p1",
        sourceId: "a",
        sourceSeq: 1,
        isFinal: false,
        text: "hello",
      }),
    (error: unknown) =>
      error instanceof EventConflictError && error.reason === "event-content"
  );
  assert.equal(store.getSnapshot("session").revision, 1);
});

test("same source kind and sequence with different event identifier is rejected", () => {
  const { store } = createStore();
  store.ingest("session", {
    eventId: "p1",
    sourceId: "a",
    sourceSeq: 1,
    isFinal: false,
    text: "hel",
  });

  assert.throws(
    () =>
      store.ingest("session", {
        eventId: "p1-conflict",
        sourceId: "a",
        sourceSeq: 1,
        isFinal: false,
        text: "hello",
      }),
    (error: unknown) =>
      error instanceof EventConflictError && error.reason === "source-sequence"
  );
});

test("older partial does not overwrite newer partial", () => {
  const { store } = createStore();
  store.ingest("session", {
    eventId: "p2",
    sourceId: "a",
    sourceSeq: 2,
    isFinal: false,
    text: "hello",
  });
  const stale = store.ingest("session", {
    eventId: "p1",
    sourceId: "a",
    sourceSeq: 1,
    isFinal: false,
    text: "h",
  });

  assert.deepEqual(stale, { status: "stale", eventId: "p1" });
  const snapshot = store.getSnapshot("session");
  assert.equal(snapshot.revision, 1);
  assert.equal(snapshot.text, "hello");
  assert.equal(snapshot.summary.eventCount, 2);
});

test("final commits ordered text and late partial cannot roll it back", () => {
  const { store } = createStore();
  assert.deepEqual(
    store.ingest("session", {
      eventId: "f1",
      sourceId: "a",
      sourceSeq: 1,
      isFinal: true,
      text: "hello ",
    }),
    { status: "inserted", eventId: "f1", revision: 1 }
  );

  assert.deepEqual(
    store.ingest("session", {
      eventId: "p2",
      sourceId: "a",
      sourceSeq: 2,
      isFinal: false,
      text: "world",
    }),
    { status: "inserted", eventId: "p2", revision: 2 }
  );

  assert.deepEqual(
    store.ingest("session", {
      eventId: "f2",
      sourceId: "a",
      sourceSeq: 2,
      isFinal: true,
      text: "world",
    }),
    { status: "inserted", eventId: "f2", revision: 3 }
  );

  const latePartial = store.ingest("session", {
    eventId: "p-old",
    sourceId: "a",
    sourceSeq: 1,
    isFinal: false,
    text: "rolled back",
  });
  assert.deepEqual(latePartial, { status: "stale", eventId: "p-old" });

  const snapshot = store.getSnapshot("session");
  assert.equal(snapshot.revision, 3);
  assert.equal(snapshot.text, "hello world");
  assert.equal(snapshot.finalText, "hello world");
  assert.deepEqual(
    snapshot.segments.map((segment) => segment.text),
    ["hello ", "world"]
  );
});

test("out-of-order finals still produce deterministic ordered final text", () => {
  const { store } = createStore();
  const events: RecognitionEventInput[] = [
    { eventId: "a1", sourceId: "a", sourceSeq: 1, isFinal: true, text: "hello " },
    { eventId: "a3", sourceId: "a", sourceSeq: 3, isFinal: true, text: "world" },
    { eventId: "a2", sourceId: "a", sourceSeq: 2, isFinal: true, text: "brave " },
  ];

  for (const event of shuffle(events, 17)) {
    store.ingest("session", event);
  }

  const snapshot = store.getSnapshot("session");
  assert.equal(snapshot.finalText, "hello brave world");
  assert.equal(snapshot.text, "hello brave world");
  assert.deepEqual(
    snapshot.segments.map((segment) => segment.sourceSeq),
    [1, 2, 3]
  );
});

test("multiple sources merge deterministically by source then sequence", () => {
  const { store } = createStore();
  const events: RecognitionEventInput[] = [
    { eventId: "b1", sourceId: "b", sourceSeq: 1, isFinal: true, text: "B1" },
    { eventId: "a2", sourceId: "a", sourceSeq: 2, isFinal: true, text: "A2" },
    { eventId: "a1", sourceId: "a", sourceSeq: 1, isFinal: true, text: "A1" },
    { eventId: "b2", sourceId: "b", sourceSeq: 2, isFinal: false, text: "B2?" },
  ];

  store.ingest("session", shuffle(events, 29));
  const snapshot = store.getSnapshot("session");
  assert.equal(snapshot.finalText, "A1A2B1");
  assert.equal(snapshot.text, "A1A2B1B2?");
  assert.deepEqual(
    snapshot.segments.map((segment) => `${segment.sourceId}:${segment.sourceSeq}:${segment.kind}`),
    ["a:1:final", "a:2:final", "b:1:final", "b:2:partial"]
  );
});

test("revisions are monotonic and historical snapshots remain queryable", () => {
  const { store } = createStore();
  store.ingest("session", {
    eventId: "p1",
    sourceId: "a",
    sourceSeq: 1,
    isFinal: false,
    text: "h",
  });
  store.ingest("session", {
    eventId: "p2",
    sourceId: "a",
    sourceSeq: 2,
    isFinal: false,
    text: "he",
  });
  store.ingest("session", {
    eventId: "f1",
    sourceId: "a",
    sourceSeq: 2,
    isFinal: true,
    text: "he",
  });

  assert.equal(store.getSnapshot("session").revision, 3);
  assert.equal(store.getRevision("session", 1).snapshot.text, "h");
  assert.equal(store.getSnapshotAt("session", 2).text, "he");
  assert.equal(store.getSnapshotAt("session", 3).finalText, "he");
});

test("consumer fetch, limit, resume, and monotonic acknowledgement work", () => {
  const { store } = createStore();
  for (let i = 1; i <= 5; i += 1) {
    store.ingest("session", {
      eventId: `e${i}`,
      sourceId: "a",
      sourceSeq: i,
      isFinal: true,
      text: `${i}`,
    });
  }

  const consumer = store.createConsumer("session", "qc");
  assert.equal(consumer.getCursor(), 0);
  assert.deepEqual(
    consumer.fetch(2).map((revision) => revision.revision),
    [1, 2]
  );
  assert.equal(consumer.acknowledge(2), 2);
  assert.deepEqual(
    consumer.fetch(2).map((revision) => revision.revision),
    [3, 4]
  );
  consumer.acknowledge(3);
  assert.equal(consumer.acknowledge(3), 3);
  assert.deepEqual(
    consumer.fetch(10).map((revision) => revision.revision),
    [4, 5]
  );
  consumer.acknowledge(5);
  assert.deepEqual(consumer.fetch(10), []);
});

test("state and consumer cursor survive close and reopen", () => {
  const { store, filename } = createStore();
  store.ingest("session", {
    eventId: "f1",
    sourceId: "a",
    sourceSeq: 1,
    isFinal: true,
    text: "persist",
  });
  store.acknowledge("session", "qc", 1);
  store.close();

  const reopened = createRecognitionStore({ filename });
  stores.push(reopened);
  assert.equal(reopened.getSnapshot("session").finalText, "persist");
  assert.equal(reopened.getCursor("session", "qc"), 1);
  assert.deepEqual(reopened.fetchChanges("session", "qc"), []);
});

test("different batches and interleaving produce the same final snapshot", () => {
  const left = createStore();
  const right = createStore();
  const events: RecognitionEventInput[] = [
    { eventId: "a1", sourceId: "a", sourceSeq: 1, isFinal: false, text: "A?" },
    { eventId: "a1f", sourceId: "a", sourceSeq: 1, isFinal: true, text: "A" },
    { eventId: "b1", sourceId: "b", sourceSeq: 1, isFinal: true, text: "B" },
    { eventId: "b2", sourceId: "b", sourceSeq: 2, isFinal: false, text: "B?" },
  ];

  left.store.ingest("session", events);
  right.store.ingest("session", [events[1]!, events[3]!]);
  right.store.ingest("session", [events[0]!, events[2]!]);

  assert.deepEqual(
    left.store.getSnapshot("session"),
    right.store.getSnapshot("session")
  );
});
