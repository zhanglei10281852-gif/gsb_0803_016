# asr-revision-hub

An embeddable TypeScript library for Node.js 20+ that merges the **partial/final**
output of several speech recognizers ("sources") for a single session into one
**queryable snapshot**, and exposes every effective change as a **durable,
strictly-increasing revision stream** for downstream consumers (e.g. a QC/质检
system). Storage is a local **SQLite** file — no Redis, Kafka, or cloud services.

It is a library, not a CLI or a service: you embed the `RevisionHub` class in
your own process.

## Why it exists

Real recognizer feeds are messy: events **duplicate**, arrive **out of order**,
and land **concurrently** (even from multiple OS processes sharing one store).
This library turns that into a consistent, replayable truth:

- **Idempotent** on exact re-delivery of the same `eventId`.
- **Rejects** the same `eventId` re-delivered with *different* content (`ConflictError`).
- A **stale partial never overwrites** newer state.
- A **final is never rolled back** by a late partial.
- The same set of events yields the **same final text, segment order, and
  summary** regardless of batching/ordering/concurrency.
- Every effective change gets a **session-monotonic `revision`**; consumers
  read by `consumerId` + durable `cursor` with **no re-sends** of acked ranges
  and **no gaps** in unacked ranges.
- State, revision log, and cursors stay **consistent across process crashes /
  power loss** — never a half-written result.

## Install / build

```bash
npm ci          # reproducible install from package-lock.json
npm run build   # compile the library to dist/
npm test        # unit + integration tests (offline)
npm run e2e     # multi-process / crash / slow-consumer simulations (offline)
```

Requires Node.js >= 20. `better-sqlite3` is the only runtime dependency.

## Integration

```ts
import { RevisionHub, ConflictError } from "asr-revision-hub";

const hub = RevisionHub.open({ path: "./data/session-store.sqlite" });

// 1) Feed recognizer events (repeated / out-of-order / concurrent is fine).
try {
  const res = hub.apply({
    sessionId: "call-42",
    sourceId: "recognizer-A",
    segmentId: "utt-7",     // one utterance being refined
    eventId: "A:utt-7:p3",  // stable & unique within (session, source)
    sourceSeq: 3,           // monotonic within the source
    kind: "partial",        // or "final"
    text: "hello wor",
    startMs: 7000,
    endMs: 7300,
  });
  // res.outcome: "created" | "updated" | "duplicate" | "superseded"
  // res.effective: true when a new revision was assigned
} catch (err) {
  if (err instanceof ConflictError) {
    // same eventId, different content — reject upstream
  } else throw err;
}

// 2) Query the current snapshot at any time.
const snap = hub.getSnapshot("call-42");
snap.headRevision;        // highest revision so far
snap.segments;            // resolved segments in deterministic order
snap.summary.fullText;    // joined transcript
snap.summary.perSource;   // per-source counts

// 3) Consume the durable revision stream.
const batch = hub.pull("call-42", "qc-consumer", { limit: 100 });
// ... process batch ...
if (batch.length) {
  hub.ack("call-42", "qc-consumer", batch[batch.length - 1].revision);
}

hub.close();
```

### Consumer loop pattern

`pull` never moves the cursor; `ack` advances it durably and monotonically.
So the safe pattern is: **pull → process → ack the last processed revision**.
If the process dies before `ack`, the same batch is re-delivered on restart
(at-least-once with idempotent processing). Acked revisions are never re-sent.

## Data model

- A **session** contains many **sources**; each source emits events for
  **segments** (utterances) identified by `segmentId`.
- The winner for a segment is chosen by a **total precedence order**
  `(final > partial, then higher sourceSeq, then eventId tie-break)`. Because
  this is a total order, folding it over the received events is
  order-independent — hence deterministic convergence.
- An `apply` that changes a segment's resolved state allocates the next
  `revision` and appends to the `revisions` table, all in one transaction.

## Durability & concurrency (key trade-offs)

- **SQLite in WAL mode** with `synchronous=FULL` (configurable to `NORMAL`).
  WAL allows a reader to proceed while a writer holds the lock and lets multiple
  OS processes share one file; `FULL` fsync survives power loss.
- Every `apply`/`ack` runs inside a single `BEGIN IMMEDIATE … COMMIT`
  transaction. `IMMEDIATE` takes the write lock up front, so **revision
  allocation is race-free across processes** and a crash leaves either the whole
  change (segment + revision + counter) or nothing — never a partial write.
  `busy_timeout` makes concurrent writers wait for the lock instead of failing.
- **Chosen trade-off:** synchronous, single-file SQLite over an external broker.
  This keeps the component embeddable and dependency-light, and gives strong
  single-node consistency. The cost is throughput ceiling of one writer at a
  time per file and no built-in cross-machine distribution — acceptable for a
  per-session aggregation component, and explicitly within the "no Redis/Kafka/
  cloud" constraint.
- **`better-sqlite3`** is synchronous. That is a feature here: it makes the
  transactional critical sections simple and correct. If you need to avoid
  blocking the event loop under heavy load, run the hub in a worker thread /
  child process (the e2e tests demonstrate multiple processes on one file).

## Public API surface

- `RevisionHub.open(options)` / `new RevisionHub(options)` / `.close()`
- `apply(event)` → `ApplyResult`, `applyBatch(events)`
- `getSnapshot(sessionId)` → `Snapshot`, `headRevision(sessionId)`
- `pull(sessionId, consumerId, opts?)` → `RevisionRecord[]`
- `ack(sessionId, consumerId, uptoRevision)` → new cursor
- `getCursor(sessionId, consumerId, autoCreate?)`
- Errors: `ConflictError`, `ValidationError` (both extend `HubError`)

See `src/types.ts` for full type definitions.

## Tests

All tests run **offline**.

- `npm test` — unit/integration: idempotency, conflict rejection, stale/final
  rules, revision monotonicity, determinism across many shuffled/duplicated
  orderings, and consumer cursor semantics (including a no-loss/no-dup fuzz).
- `npm run e2e` — simulations:
  - **Multi-instance concurrency:** 5 child processes write the same logical
    stream (with 25% overlapping deliveries) to one SQLite file; the resolved
    snapshot must equal a single-process baseline and the revision log must be
    dense with no gaps.
  - **Power loss:** a worker is `SIGKILL`-ed mid-stream; the store must be
    internally consistent and converge to the pristine baseline on replay.
  - **Slow consumer:** a producer streams while a deliberately slow consumer
    pulls/acks in small batches, plus a consumer-restart test — every revision
    delivered exactly once, in order, resuming at the durable cursor.

## License

MIT
