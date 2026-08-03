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
- **Human review with correction leases**: a reviewer takes a time-boxed lease
  on a segment (based on a `baseRevision`), and only the lease holder may submit
  a correction. Competing claims yield a single winner; expired or stale-base
  submits fail with **distinguishable** errors. Corrections don't rewrite the
  raw recognition — they keep `actor`/`reason`/`supersedes` lineage and land as
  **new revisions in the same stream** existing consumers already read.
- State, revision log, cursors, leases, and corrections stay **consistent
  across process crashes / power loss** — never a half-written result.

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

### Human review: correction leases

Corrections flow through the *same* revision stream, so a consumer that already
reads recognition revisions automatically receives corrections too — it just
sees records whose `origin` is `"correction"` carrying `actor`, `reason`,
`correctionId`, and `supersedesRevision`.

```ts
// A reviewer sees the segment at some baseRevision and claims a time-boxed lease.
const snap = hub.getSnapshot("call-42");
const seg = snap.segments.find((s) => s.segmentId === "utt-7")!;

const lease = hub.acquireLease({
  sessionId: "call-42",
  sourceId: seg.sourceId,
  segmentId: "utt-7",
  actor: "reviewer-1",
  baseRevision: seg.revision, // the state being corrected
  ttlMs: 60_000,
});
// A competing acquireLease while this lease is live throws LeaseConflictError.

try {
  const res = hub.submitCorrection({
    leaseId: lease.leaseId,
    actor: "reviewer-1",
    text: "hello world",
    reason: "spelling",
    correctionId: "opt-idempotency-key", // optional; re-submit is idempotent
  });
  // res.revision is the new stream revision; res.supersedesRevision === baseRevision
} catch (err) {
  // Distinguishable outcomes (all extend HubError, each with a distinct .code):
  //  - LeaseExpiredError  ("LEASE_EXPIRED"): the TTL elapsed before submit
  //  - StaleBaseError     ("STALE_BASE"):    a newer revision landed since the lease
  //  - NoLeaseError       ("NO_LEASE"):      unknown lease / wrong actor / released
}
```

Guarantees:

- **Single winner:** while a lease is live, only its holder can submit; other
  acquisitions get `LeaseConflictError`. An *expired* lease can be taken over.
- **Base enforcement:** a submit succeeds only if the segment is still at the
  lease's `baseRevision`; otherwise `StaleBaseError` — so a correction built on a
  stale view never silently clobbers newer state.
- **Raw recognition preserved:** corrections never touch the `events` table.
  Lineage (`actor`, `reason`, `supersedes`, `leaseId`) is recorded in a separate
  `corrections` table, and the segment flips to `origin="correction"`.
- **Corrections are authoritative:** once a segment is corrected, later
  recognition events are `superseded` (extends the final-no-rollback rule).

### Compliance handoff: session archives

To move a whole session into an isolated environment, export it to a
self-contained, versioned, tamper-evident archive (newline-delimited JSON) and
import it into an empty store. The archive carries raw events, resolved
segments, the full revision history, the **complete correction lineage**
(`actor`, `baseRevision`, `reason`, `supersedes`), leases, and consumer cursors.

```ts
// Export side — streaming, memory-bounded (generator over SQLite cursors).
for (const line of hub.exportSession("call-42")) writeToFile(line + "\n");
// or, as one string:
const archive = hub.exportSessionToString("call-42");
// Optionally key the integrity chain with a shared secret (HMAC):
const keyed = hub.exportSessionToString("call-42", { secret: process.env.HANDOFF_KEY });

// Import side — into an empty DB. Streamed input (any chunking) is accepted.
const res = isolatedHub.importSession(archive); // or importSession(chunkIterable)
// res.imported === true on first import; false (no-op) on identical re-import.
```

After importing into an empty DB, the **snapshot, revision history, and
resumable cursor positions are equivalent** to the export side — a consumer
resumes exactly where it left off.

Integrity guarantees (all fail *before any state is visible* — the import is one
transaction and rolls back on any failure, leaving no half-imported session):

- **Versioned:** the header carries a `format` version; unsupported versions are
  rejected. Unknown *optional* fields on any record are preserved and re-emitted
  on the next export, leaving room for later versions.
- **Tamper-evident:** a trailer pins the body record `count`, a `content` digest
  (idempotency key, header-time-independent), and an order-sensitive `chain`
  digest over every line. Reordering, mutation, or truncation → the digests or
  count mismatch → `ArchiveIntegrityError`. With `{ secret }`, the digests are
  HMACs, so only a holder of the secret can produce a verifying archive.
- **Idempotent:** re-importing the identical archive is a no-op
  (`imported: false`). A *different* archive for a session that already has
  content → `ArchiveConflictError` (it never silently overwrites).

## Data model

- A **session** contains many **sources**; each source emits events for
  **segments** (utterances) identified by `segmentId`.
- The winner for a segment is chosen by a **total precedence order**
  `(final > partial, then higher sourceSeq, then eventId tie-break)`. Because
  this is a total order, folding it over the received events is
  order-independent — hence deterministic convergence. A human correction
  outranks all recognition for that segment.
- An `apply` (recognition) or `submitCorrection` (review) that changes a
  segment's resolved state allocates the next `revision` and appends to the
  shared `revisions` table, all in one transaction.

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
  - `options`: `{ path, synchronous?, busyTimeoutMs?, clock? }` (`clock` injects a
    time source, mainly for deterministic lease-TTL tests).
- `apply(event)` → `ApplyResult`, `applyBatch(events)`
- `getSnapshot(sessionId)` → `Snapshot`, `headRevision(sessionId)`
- `pull(sessionId, consumerId, opts?)` → `RevisionRecord[]`
- `ack(sessionId, consumerId, uptoRevision)` → new cursor
- `getCursor(sessionId, consumerId, autoCreate?)`
- Human review: `acquireLease(req)` → `Lease`, `submitCorrection(req)` →
  `CorrectionResult`, `getLease(sessionId, sourceId, segmentId)`,
  `releaseLease(leaseId, actor)`
- Handoff: `exportSession(sessionId, opts?)` → `IterableIterator<string>`,
  `exportSessionToString(sessionId, opts?)` → `string`,
  `importSession(source, opts?)` → `ImportResult` (`source` is a string or an
  `Iterable<string>` of chunks; `opts` is `{ secret? }`)
- Errors (all extend `HubError` with a `.code`): `ConflictError`,
  `ValidationError`, `LeaseConflictError`, `LeaseExpiredError`, `NoLeaseError`,
  `StaleBaseError`, `ArchiveIntegrityError`, `ArchiveConflictError`

Existing older stores are migrated in place on open (v1→v2 adds correction
provenance columns; v2→v3 adds the archive ledger tables); the previously
documented API is unchanged and remains source-compatible.

See `src/types.ts` for full type definitions.

## Tests

All tests run **offline**.

- `npm test` — unit/integration: idempotency, conflict rejection, stale/final
  rules, revision monotonicity, determinism across many shuffled/duplicated
  orderings, consumer cursor semantics (including a no-loss/no-dup fuzz), the
  full correction/lease flow (single-winner, TTL expiry, stale base, no-rollback,
  idempotent re-submit, supersedes lineage), schema migration, and the archive
  flow (round-trip equivalence, idempotent re-import, conflict, deterministic
  digest, reorder/truncate/tamper/version rejection, HMAC secret, streamed
  chunking, forward-compat unknown fields).
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
  - **Lease contention:** 6 reviewer processes concurrently contend for leases
    on shared segments; exactly one correction wins per base revision, the
    stream stays dense, every segment resolves to a correction winner, and raw
    events remain untouched.
  - **Correction power loss:** a reviewer process is `SIGKILL`-ed mid-correction;
    the store stays consistent (no segment flipped without its lineage +
    stream revision) and the review completes on resume.
  - **Archive fault injection:** an import child process is `SIGKILL`-ed before
    it can commit — the target DB shows no session at all — then a clean
    re-import restores state equivalent to the source and a second import
    no-ops. A separate case races two importer processes on one file and asserts
    exactly one writes while the other is an idempotent no-op.

## License

MIT
