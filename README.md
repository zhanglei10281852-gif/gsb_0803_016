# ASR Revision Store

An embeddable TypeScript/Node.js 20 library for consolidating partial and final speech-recognition fragments from multiple sources into queryable session snapshots and a durable revision stream. It also supports human-review corrections with time-bounded leases, actor/reason audit records, and supersession lineage.

## Integration

Install the package into an existing Node.js 20 application and open one store per process against the same SQLite file when necessary.

```ts
import { createRecognitionStore } from "asr-revision-store";

const store = createRecognitionStore({ filename: "./data/asr.db" });

store.ingest("session-1", {
  eventId: "source-a-1",
  sourceId: "source-a",
  sourceSeq: 1,
  isFinal: false,
  text: "hello",
});

const snapshot = store.getSnapshot("session-1");
console.log(snapshot.text, snapshot.finalText, snapshot.revision);

const consumer = store.createConsumer("session-1", "quality-control-1");
const changes = consumer.fetch(100);
for (const change of changes) {
  await downstream.apply(change.snapshot);
  consumer.acknowledge(change.revision);
}
```

`ingest` accepts one event or an array. Arrays are canonicalized before the transaction so splitting or reordering batches cannot change the final snapshot.

## Semantics

- `eventId` identifies an immutable event. Repeating it with identical content is idempotent; repeating it with different content throws `EventConflictError`.
- `sourceSeq` is monotonic per source. Two events with the same source, kind, and sequence but different event IDs are rejected.
- A newer partial supersedes older partials. An older or finalized partial is accepted for audit purposes but has no snapshot effect and receives no revision.
- Final fragments are immutable and ordered by `sourceId`, then `sourceSeq`. A late partial at or below the final boundary cannot roll final state back.
- Every effective change receives a session-local strictly increasing revision. Each revision stores the complete post-change snapshot.
- Consumers use `consumerId` and an acknowledgement cursor. `fetch` returns revisions after the cursor; `acknowledge` only advances the cursor monotonically. Unacknowledged revisions can be redelivered, while acknowledged revisions are not returned again.
- All effective state changes, revisions, and cursor updates are committed in SQLite transactions. File databases use WAL and `synchronous=FULL`; in-memory databases use the memory journal.

## Human review corrections

Reviewers operate on finalized segments and must acquire a time-bounded lease before submitting a correction. The lease prevents two reviewers from editing the same segment concurrently. Corrections never mutate the original recognition events; they add a new durable record with actor, reason, base revision, and a `supersedes` link to the prior correction (if any). Each accepted correction is emitted as a new revision in the same revision stream, so existing downstream consumers automatically receive it on their next `fetch`.

```ts
const current = store.getSnapshot("session-1");
const claim = store.claimLease("session-1", {
  sourceId: "source-a",
  sourceSeq: 0,
  actor: "reviewer-12",
  baseRevision: current.revision,
  ttlMs: 60_000,
});

if (!claim.acquired) {
  // another reviewer holds the active lease; retry later
} else {
  const result = store.submitCorrection("session-1", {
    leaseId: claim.lease.leaseId,
    actor: "reviewer-12",
    reason: "proper noun spelling",
    correctedText: "New York",
    baseRevision: current.revision,
  });

  // result.revision is the new session revision delivered to consumers.
}
```

Conflict outcomes are all instances of `ReviewConflictError` with a distinct `reason`:

- `lease-taken`: another actor holds an active lease for the segment.
- `lease-expired`: the lease has expired or was released before submission.
- `lease-not-found` / `lease-actor-mismatch`: the lease does not exist or belongs to another actor.
- `base-revision-stale`: the revision advanced after the lease was granted (e.g. another correction landed); the reviewer must refetch and re-claim.
- `segment-not-found`: the referenced final segment does not exist.

Additional review methods:

- `getLease(sessionId, leaseId)` and `getActiveLease(sessionId, sourceId, sourceSeq)` inspect lease state.
- `releaseLease(sessionId, leaseId, actor)` voluntarily returns a lease.
- `getCorrection(sessionId, correctionId)` and `getCorrectionLineage(sessionId, sourceId, sourceSeq)` return the audit chain.
- In snapshots, corrected segments expose `text` (effective text), `originalText` (recognition result), and an optional `correction` object with actor, reason, base revision, supersedes id, and timestamps.

## Key trade-offs

- `better-sqlite3` provides synchronous local transactions and native SQLite durability. It is appropriate for embedding but not for environments that cannot load native modules.
- Segment order is deterministic rather than wall-clock ordered. The public ordering is `sourceId`, then `sourceSeq`; there is no reliance on arrival time or external timestamps.
- Revisions are full snapshots, not operational diffs. This makes consumers and replay simple and resilient, at the cost of additional storage for high-volume sessions.
- Stale partials are stored without a revision so duplicate replay remains idempotent and conflict detection remains complete, while final summaries ignore them.
- Corrections are additive: recognition rows are never overwritten, and the full correction lineage is queryable. The effective snapshot always shows the latest correction per segment; `originalText` preserves the ASR output.
- Retention is not automatic. Revisions remain available until the application chooses to archive or delete them, which keeps slow consumers from losing unread changes.

## Commands

```sh
npm ci
npm run build
npm test
npm run e2e
```

Tests run entirely offline with local temporary SQLite databases. Unit tests cover idempotency, conflicts, stale partials, final immutability, deterministic snapshots, revisions, consumer cursors, lease competition, lease expiry, stale-base-revision conflicts, and correction lineage. End-to-end tests spawn multiple processes, force-kill a writer mid-ingest, reopen the database, and run a slow page-limited consumer.
