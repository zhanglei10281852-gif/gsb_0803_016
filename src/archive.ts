import { createHash } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import type { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createInterface } from "node:readline";
import type Database from "better-sqlite3";
import { SCHEMA_VERSION } from "./schema";
import {
  RecognitionStoreError,
  ReviewConflictError,
} from "./errors";
import {
  ARCHIVE_FORMAT_VERSION,
  ARCHIVE_MAGIC,
  type ArchiveCorrectionRecord,
  type ArchiveCursorRecord,
  type ArchiveDataRecord,
  type ArchiveEventRecord,
  type ArchiveFooter,
  type ArchiveHeader,
  type ArchiveImportResult,
  type ArchiveLeaseRecord,
  type ArchiveRevisionRecord,
  type SessionArchiveStream,
} from "./archive-types";

export class ArchiveIntegrityError extends RecognitionStoreError {
  public readonly reason:
    | "bad-magic"
    | "unsupported-version"
    | "bad-header"
    | "bad-footer"
    | "hash-mismatch"
    | "truncated"
    | "reorder"
    | "invalid-json"
    | "session-mismatch";

  constructor(reason: ArchiveIntegrityError["reason"], message: string) {
    super(message);
    this.name = "ArchiveIntegrityError";
    this.reason = reason;
  }
}

export class ArchiveImportConflictError extends RecognitionStoreError {
  public readonly conflictingTable: string;
  public readonly conflictingKey: string;

  constructor(table: string, key: string, message: string) {
    super(message);
    this.name = "ArchiveImportConflictError";
    this.conflictingTable = table;
    this.conflictingKey = key;
  }
}

interface EventRow {
  event_id: string;
  source_id: string;
  source_seq: number;
  kind: "final" | "partial";
  text: string;
  created_at: number;
}

interface RevisionExportRow {
  revision: number;
  change_type: "event" | "correction";
  event_id: string | null;
  source_id: string | null;
  source_seq: number | null;
  kind: "final" | "partial" | null;
  correction_id: string | null;
  snapshot: string;
  created_at: number;
}

interface CorrectionExportRow {
  correction_id: string;
  source_id: string;
  source_seq: number;
  actor: string;
  reason: string;
  original_text: string;
  corrected_text: string;
  base_revision: number;
  supersedes: string | null;
  lease_id: string;
  created_at: number;
}

interface LeaseExportRow {
  lease_id: string;
  source_id: string;
  source_seq: number;
  actor: string;
  base_revision: number;
  ttl_ms: number;
  claimed_at: number;
  expires_at: number;
  released_at: number | null;
}

interface CursorExportRow {
  consumer_id: string;
  cursor: number;
  updated_at: number;
}

interface ExtrasRow {
  record_table: string;
  record_key: string;
  extras: string;
}

interface UnknownRow {
  seq: number;
  record_type: string;
  payload: string;
}

const KNOWN_TYPES = new Set([
  "event",
  "revision",
  "correction",
  "lease",
  "cursor",
]);

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return "[" + value.map(stableStringify).join(",") + "]";
  }
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([k, v]) => JSON.stringify(k) + ":" + stableStringify(v));
  return "{" + entries.join(",") + "}";
}

function canonicalLine(record: ArchiveDataRecord): string {
  return stableStringify(record);
}

function hashLines(lines: string[], algorithm: "sha256"): string {
  const hash = createHash(algorithm);
  for (const line of lines) {
    hash.update(line);
    hash.update("\n");
  }
  return hash.digest("hex");
}

export async function* exportSessionArchive(
  db: Database.Database,
  sessionId: string,
  clock: () => number = Date.now
): SessionArchiveStream {
  const events = db
    .prepare<[string], EventRow>(
      `SELECT event_id, source_id, source_seq, kind, text, created_at
       FROM events WHERE session_id = ?
       ORDER BY source_id, source_seq, kind, event_id`
    )
    .all(sessionId);

  const revisions = db
    .prepare<[string], RevisionExportRow>(
      `SELECT revision, change_type, event_id, source_id, source_seq,
              kind, correction_id, snapshot, created_at
       FROM revisions WHERE session_id = ?
       ORDER BY revision ASC`
    )
    .all(sessionId);

  const corrections = db
    .prepare<[string], CorrectionExportRow>(
      `SELECT correction_id, source_id, source_seq, actor, reason,
              original_text, corrected_text, base_revision, supersedes,
              lease_id, created_at
       FROM corrections WHERE session_id = ?
       ORDER BY created_at ASC, correction_id ASC`
    )
    .all(sessionId);

  const leases = db
    .prepare<[string], LeaseExportRow>(
      `SELECT lease_id, source_id, source_seq, actor, base_revision,
              ttl_ms, claimed_at, expires_at, released_at
       FROM review_leases WHERE session_id = ?
       ORDER BY source_id, source_seq`
    )
    .all(sessionId);

  const cursors = db
    .prepare<[string], CursorExportRow>(
      `SELECT consumer_id, cursor, updated_at
       FROM consumer_cursors WHERE session_id = ?
       ORDER BY consumer_id ASC`
    )
    .all(sessionId);

  const extrasRows = db
    .prepare<[string], ExtrasRow>(
      `SELECT record_table, record_key, extras
       FROM archive_extras WHERE session_id = ?`
    )
    .all(sessionId);
  const extrasMap = new Map<string, Record<string, unknown>>();
  for (const row of extrasRows) {
    const key = `${row.record_table}\u0000${row.record_key}`;
    extrasMap.set(key, JSON.parse(row.extras) as Record<string, unknown>);
  }

  const unknownRows = db
    .prepare<[string], UnknownRow>(
      `SELECT seq, record_type, payload
       FROM archive_unknown_records WHERE session_id = ?
       ORDER BY seq ASC`
    )
    .all(sessionId);

  const records: ArchiveDataRecord[] = [];

  for (const row of events) {
    const base: ArchiveEventRecord = {
      eventId: row.event_id,
      sourceId: row.source_id,
      sourceSeq: row.source_seq,
      kind: row.kind,
      text: row.text,
      createdAt: row.created_at,
    };
    const extras = extrasMap.get(`event\u0000${row.event_id}`);
    records.push({
      type: "event",
      data: extras ? { ...base, ...extras } : base,
    });
  }

  for (const row of revisions) {
    const base: ArchiveRevisionRecord = {
      revision: row.revision,
      changeType: row.change_type,
      ...(row.event_id === null ? {} : { eventId: row.event_id }),
      ...(row.source_id === null ? {} : { sourceId: row.source_id }),
      ...(row.source_seq === null ? {} : { sourceSeq: row.source_seq }),
      ...(row.kind === null ? {} : { kind: row.kind }),
      ...(row.correction_id === null
        ? {}
        : { correctionId: row.correction_id }),
      snapshot: JSON.parse(row.snapshot) as unknown,
      createdAt: row.created_at,
    };
    const extras = extrasMap.get(`revision\u0000${String(row.revision)}`);
    records.push({
      type: "revision",
      data: extras ? { ...base, ...extras } : base,
    });
  }

  for (const row of corrections) {
    const base: ArchiveCorrectionRecord = {
      correctionId: row.correction_id,
      sourceId: row.source_id,
      sourceSeq: row.source_seq,
      actor: row.actor,
      reason: row.reason,
      originalText: row.original_text,
      correctedText: row.corrected_text,
      baseRevision: row.base_revision,
      ...(row.supersedes === null ? {} : { supersedes: row.supersedes }),
      leaseId: row.lease_id,
      createdAt: row.created_at,
    };
    const extras = extrasMap.get(`correction\u0000${row.correction_id}`);
    records.push({
      type: "correction",
      data: extras ? { ...base, ...extras } : base,
    });
  }

  for (const row of leases) {
    const base: ArchiveLeaseRecord = {
      leaseId: row.lease_id,
      sourceId: row.source_id,
      sourceSeq: row.source_seq,
      actor: row.actor,
      baseRevision: row.base_revision,
      ttlMs: row.ttl_ms,
      claimedAt: row.claimed_at,
      expiresAt: row.expires_at,
      ...(row.released_at === null ? {} : { releasedAt: row.released_at }),
    };
    const extras = extrasMap.get(`lease\u0000${row.lease_id}`);
    records.push({
      type: "lease",
      data: extras ? { ...base, ...extras } : base,
    });
  }

  for (const row of cursors) {
    const base: ArchiveCursorRecord = {
      consumerId: row.consumer_id,
      cursor: row.cursor,
      updatedAt: row.updated_at,
    };
    const extras = extrasMap.get(`cursor\u0000${row.consumer_id}`);
    records.push({
      type: "cursor",
      data: extras ? { ...base, ...extras } : base,
    });
  }

  for (const row of unknownRows) {
    records.push({
      type: row.record_type,
      data: JSON.parse(row.payload) as Record<string, unknown>,
    });
  }

  const canonicalRecords = records.map(canonicalLine);
  const dataHash = hashLines(canonicalRecords, "sha256");

  const header: ArchiveHeader = {
    magic: ARCHIVE_MAGIC,
    archiveVersion: ARCHIVE_FORMAT_VERSION,
    libraryVersion: SCHEMA_VERSION,
    sessionId,
    createdAt: clock(),
    recordCount: records.length,
    dataHashAlgorithm: "sha256",
    dataHash,
  };

  yield stableStringify(header) + "\n";
  for (let i = 0; i < records.length; i += 1) {
    const line = canonicalRecords[i] as string;
    yield line + "\n";
  }

  const footer: ArchiveFooter = {
    end: true,
    recordCount: records.length,
    dataHash,
  };
  yield stableStringify(footer) + "\n";
}

export async function writeSessionArchive(
  db: Database.Database,
  sessionId: string,
  outputPath: string,
  clock: () => number = Date.now
): Promise<void> {
  const writeStream = createWriteStream(outputPath);
  try {
    for await (const chunk of exportSessionArchive(db, sessionId, clock)) {
      if (!writeStream.write(chunk)) {
        await new Promise<void>((resolve, reject) =>
          writeStream.once("drain", resolve).once("error", reject)
        );
      }
    }
  } finally {
    await new Promise<void>((resolve) => writeStream.end(resolve));
  }
}

type ParsedLine =
  | { kind: "header"; value: ArchiveHeader }
  | { kind: "data"; seq: number; value: ArchiveDataRecord; raw: string }
  | { kind: "footer"; value: ArchiveFooter };

async function readArchiveLines(
  stream: Readable
): Promise<{ header: ArchiveHeader; data: Array<{ value: ArchiveDataRecord; raw: string }> }> {
  const rl = createInterface({ input: stream, crlfDelay: Infinity });
  const dataLines: string[] = [];
  let header: ArchiveHeader | undefined;
  let footer: ArchiveFooter | undefined;
  let lineNumber = 0;

  for await (const line of rl) {
    lineNumber += 1;
    const trimmed = line.trim();
    if (!trimmed) continue;

    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      throw new ArchiveIntegrityError(
        "invalid-json",
        `Archive line ${lineNumber} is not valid JSON.`
      );
    }

    if (
      parsed &&
      typeof parsed === "object" &&
      "magic" in parsed &&
      (parsed as { magic?: unknown }).magic === ARCHIVE_MAGIC
    ) {
      if (header) {
        throw new ArchiveIntegrityError(
          "bad-header",
          "Archive contains multiple headers."
        );
      }
      header = parsed as ArchiveHeader;
      continue;
    }

    if (
      parsed &&
      typeof parsed === "object" &&
      "end" in parsed &&
      (parsed as { end?: unknown }).end === true
    ) {
      if (footer) {
        throw new ArchiveIntegrityError(
          "bad-footer",
          "Archive contains multiple footers."
        );
      }
      footer = parsed as ArchiveFooter;
      continue;
    }

    dataLines.push(trimmed);
  }

  if (!header) {
    throw new ArchiveIntegrityError("bad-header", "Archive header is missing.");
  }
  if (!footer) {
    throw new ArchiveIntegrityError(
      "truncated",
      "Archive footer is missing; the archive may be truncated."
    );
  }
  if (header.magic !== ARCHIVE_MAGIC) {
    throw new ArchiveIntegrityError(
      "bad-magic",
      `Unrecognized archive magic: ${String(header.magic)}.`
    );
  }
  if (header.archiveVersion !== ARCHIVE_FORMAT_VERSION) {
    throw new ArchiveIntegrityError(
      "unsupported-version",
      `Archive version ${String(header.archiveVersion)} is not supported.`
    );
  }
  if (footer.recordCount !== dataLines.length) {
    throw new ArchiveIntegrityError(
      "truncated",
      `Footer reports ${footer.recordCount} records but found ${dataLines.length}.`
    );
  }
  if (header.recordCount !== dataLines.length) {
    throw new ArchiveIntegrityError(
      "truncated",
      `Header reports ${header.recordCount} records but found ${dataLines.length}.`
    );
  }

  const data: Array<{ value: ArchiveDataRecord; raw: string }> = [];
  for (let i = 0; i < dataLines.length; i += 1) {
    const raw = dataLines[i] as string;
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw new ArchiveIntegrityError(
        "invalid-json",
        `Data line ${i + 1} is not valid JSON.`
      );
    }
    if (
      !parsed ||
      typeof parsed !== "object" ||
      typeof (parsed as { type?: unknown }).type !== "string"
    ) {
      throw new ArchiveIntegrityError(
        "bad-header",
        `Data line ${i + 1} is missing a string type.`
      );
    }
    data.push({ value: parsed as ArchiveDataRecord, raw });
  }

  const recomputed = hashLines(data.map((d) => d.raw), "sha256");
  if (recomputed !== header.dataHash) {
    throw new ArchiveIntegrityError(
      "hash-mismatch",
      "Archive data hash does not match header; archive may be tampered or reordered."
    );
  }
  if (recomputed !== footer.dataHash) {
    throw new ArchiveIntegrityError(
      "hash-mismatch",
      "Archive data hash does not match footer."
    );
  }

  return { header, data };
}

function pickExtras<T extends Record<string, unknown>>(
  value: T,
  known: ReadonlySet<string>
): Record<string, unknown> | undefined {
  const extras: Record<string, unknown> = {};
  let hasExtras = false;
  for (const [key, val] of Object.entries(value)) {
    if (!known.has(key)) {
      extras[key] = val;
      hasExtras = true;
    }
  }
  return hasExtras ? extras : undefined;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value)
  );
}

function assertObject(
  value: unknown,
  label: string
): asserts value is Record<string, unknown> {
  if (!isPlainObject(value)) {
    throw new ArchiveIntegrityError(
      "bad-header",
      `${label} must be a JSON object.`
    );
  }
}

function assertString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new ArchiveIntegrityError(
      "bad-header",
      `${label} must be a non-empty string.`
    );
  }
  return value;
}

function assertInteger(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new ArchiveIntegrityError(
      "bad-header",
      `${label} must be a safe integer.`
    );
  }
  return value;
}

function validateEvent(rec: ArchiveDataRecord): ArchiveEventRecord {
  assertObject(rec.data, "event data");
  const data = rec.data;
  return {
    eventId: assertString(data.eventId, "eventId"),
    sourceId: assertString(data.sourceId, "sourceId"),
    sourceSeq: assertInteger(data.sourceSeq, "sourceSeq"),
    kind: data.kind === "final" || data.kind === "partial"
      ? data.kind
      : (() => {
          throw new ArchiveIntegrityError(
            "bad-header",
            "event kind must be 'final' or 'partial'."
          );
        })(),
    text: typeof data.text === "string" ? data.text : "",
    createdAt: assertInteger(data.createdAt, "createdAt"),
  };
}

function validateRevision(rec: ArchiveDataRecord): ArchiveRevisionRecord {
  assertObject(rec.data, "revision data");
  const data = rec.data;
  const result: ArchiveRevisionRecord = {
    revision: assertInteger(data.revision, "revision"),
    changeType:
      data.changeType === "event" || data.changeType === "correction"
        ? data.changeType
        : (() => {
            throw new ArchiveIntegrityError(
              "bad-header",
              "revision changeType must be 'event' or 'correction'."
            );
          })(),
    snapshot: data.snapshot,
    createdAt: assertInteger(data.createdAt, "createdAt"),
  };
  if (typeof data.eventId === "string") result.eventId = data.eventId;
  if (typeof data.sourceId === "string") result.sourceId = data.sourceId;
  if (Number.isSafeInteger(data.sourceSeq))
    result.sourceSeq = data.sourceSeq as number;
  if (data.kind === "final" || data.kind === "partial")
    result.kind = data.kind;
  if (typeof data.correctionId === "string")
    result.correctionId = data.correctionId;
  if (result.revision <= 0) {
    throw new ArchiveIntegrityError(
      "bad-header",
      "revision number must be positive."
    );
  }
  return result;
}

function validateCorrection(
  rec: ArchiveDataRecord
): ArchiveCorrectionRecord {
  assertObject(rec.data, "correction data");
  const data = rec.data;
  const result: ArchiveCorrectionRecord = {
    correctionId: assertString(data.correctionId, "correctionId"),
    sourceId: assertString(data.sourceId, "sourceId"),
    sourceSeq: assertInteger(data.sourceSeq, "sourceSeq"),
    actor: assertString(data.actor, "actor"),
    reason: assertString(data.reason, "reason"),
    originalText:
      typeof data.originalText === "string" ? data.originalText : "",
    correctedText:
      typeof data.correctedText === "string" ? data.correctedText : "",
    baseRevision: assertInteger(data.baseRevision, "baseRevision"),
    leaseId: assertString(data.leaseId, "leaseId"),
    createdAt: assertInteger(data.createdAt, "createdAt"),
  };
  if (typeof data.supersedes === "string") result.supersedes = data.supersedes;
  return result;
}

function validateLease(rec: ArchiveDataRecord): ArchiveLeaseRecord {
  assertObject(rec.data, "lease data");
  const data = rec.data;
  const result: ArchiveLeaseRecord = {
    leaseId: assertString(data.leaseId, "leaseId"),
    sourceId: assertString(data.sourceId, "sourceId"),
    sourceSeq: assertInteger(data.sourceSeq, "sourceSeq"),
    actor: assertString(data.actor, "actor"),
    baseRevision: assertInteger(data.baseRevision, "baseRevision"),
    ttlMs: assertInteger(data.ttlMs, "ttlMs"),
    claimedAt: assertInteger(data.claimedAt, "claimedAt"),
    expiresAt: assertInteger(data.expiresAt, "expiresAt"),
  };
  if (Number.isSafeInteger(data.releasedAt))
    result.releasedAt = data.releasedAt as number;
  return result;
}

function validateCursor(rec: ArchiveDataRecord): ArchiveCursorRecord {
  assertObject(rec.data, "cursor data");
  const data = rec.data;
  return {
    consumerId: assertString(data.consumerId, "consumerId"),
    cursor: assertInteger(data.cursor, "cursor"),
    updatedAt: assertInteger(data.updatedAt, "updatedAt"),
  };
}

const EVENT_KNOWN = new Set([
  "eventId",
  "sourceId",
  "sourceSeq",
  "kind",
  "text",
  "createdAt",
]);
const REVISION_KNOWN = new Set([
  "revision",
  "changeType",
  "eventId",
  "sourceId",
  "sourceSeq",
  "kind",
  "correctionId",
  "snapshot",
  "createdAt",
]);
const CORRECTION_KNOWN = new Set([
  "correctionId",
  "sourceId",
  "sourceSeq",
  "actor",
  "reason",
  "originalText",
  "correctedText",
  "baseRevision",
  "supersedes",
  "leaseId",
  "createdAt",
]);
const LEASE_KNOWN = new Set([
  "leaseId",
  "sourceId",
  "sourceSeq",
  "actor",
  "baseRevision",
  "ttlMs",
  "claimedAt",
  "expiresAt",
  "releasedAt",
]);
const CURSOR_KNOWN = new Set(["consumerId", "cursor", "updatedAt"]);

export interface ImportSessionArchiveOptions {
  injectFailureAfterRecords?: number;
}

export function importSessionArchive(
  db: Database.Database,
  targetSessionId: string,
  stream: Readable,
  options: ImportSessionArchiveOptions = {}
): Promise<ArchiveImportResult> {
  return importSessionArchiveInternal(
    db,
    targetSessionId,
    stream,
    options
  );
}

async function importSessionArchiveInternal(
  db: Database.Database,
  targetSessionId: string,
  stream: Readable,
  options: ImportSessionArchiveOptions
): Promise<ArchiveImportResult> {
  const { header, data } = await readArchiveLines(stream);

  if (header.sessionId !== targetSessionId) {
    throw new ArchiveIntegrityError(
      "session-mismatch",
      `Archive is for session ${header.sessionId}, cannot import into ${targetSessionId}.`
    );
  }

  let revision = 1;

  const events: ArchiveEventRecord[] = [];
  const revisions: ArchiveRevisionRecord[] = [];
  const corrections: ArchiveCorrectionRecord[] = [];
  const leases: ArchiveLeaseRecord[] = [];
  const cursors: ArchiveCursorRecord[] = [];
  const unknown: Array<{ type: string; payload: Record<string, unknown>; seq: number }> = [];

  const extrasToWrite: Array<{
    table: string;
    key: string;
    extras: Record<string, unknown>;
  }> = [];

  for (let i = 0; i < data.length; i += 1) {
    const record = data[i]!.value;
    if (typeof record.type !== "string" || !record.data) {
      throw new ArchiveIntegrityError(
        "bad-header",
        `Data record ${i + 1} is malformed.`
      );
    }

    if (options.injectFailureAfterRecords !== undefined && i >= options.injectFailureAfterRecords) {
      throw new Error("injected import failure");
    }

    if (record.type === "event") {
      const event = validateEvent(record);
      events.push(event);
      const extras = pickExtras(
        record.data as Record<string, unknown>,
        EVENT_KNOWN
      );
      if (extras) extrasToWrite.push({ table: "event", key: event.eventId, extras });
    } else if (record.type === "revision") {
      const rev = validateRevision(record);
      if (rev.revision !== revision) {
        throw new ArchiveIntegrityError(
          "reorder",
          `Revisions must be contiguous; expected ${revision}, got ${rev.revision}.`
        );
      }
      revision += 1;
      revisions.push(rev);
      const extras = pickExtras(
        record.data as Record<string, unknown>,
        REVISION_KNOWN
      );
      if (extras)
        extrasToWrite.push({
          table: "revision",
          key: String(rev.revision),
          extras,
        });
    } else if (record.type === "correction") {
      const correction = validateCorrection(record);
      corrections.push(correction);
      const extras = pickExtras(
        record.data as Record<string, unknown>,
        CORRECTION_KNOWN
      );
      if (extras)
        extrasToWrite.push({
          table: "correction",
          key: correction.correctionId,
          extras,
        });
    } else if (record.type === "lease") {
      const lease = validateLease(record);
      leases.push(lease);
      const extras = pickExtras(
        record.data as Record<string, unknown>,
        LEASE_KNOWN
      );
      if (extras)
        extrasToWrite.push({
          table: "lease",
          key: lease.leaseId,
          extras,
        });
    } else if (record.type === "cursor") {
      const cursor = validateCursor(record);
      cursors.push(cursor);
      const extras = pickExtras(
        record.data as Record<string, unknown>,
        CURSOR_KNOWN
      );
      if (extras)
        extrasToWrite.push({
          table: "cursor",
          key: cursor.consumerId,
          extras,
        });
    } else if (!KNOWN_TYPES.has(record.type)) {
      assertObject(record.data, `unknown ${record.type} data`);
      unknown.push({
        type: record.type,
        payload: record.data,
        seq: i + 1,
      });
    } else {
      throw new ArchiveIntegrityError(
        "bad-header",
        `Unknown record type: ${record.type}.`
      );
    }
  }

  // Validate referential integrity before writing anything.
  for (const rev of revisions) {
    if (rev.changeType === "correction") {
      if (!rev.correctionId) {
        throw new ArchiveIntegrityError(
          "bad-header",
          `Revision ${rev.revision} correction is missing correctionId.`
        );
      }
      const exists = corrections.some(
        (c) => c.correctionId === rev.correctionId
      );
      if (!exists) {
        throw new ArchiveIntegrityError(
          "bad-header",
          `Revision ${rev.revision} references unknown correction ${rev.correctionId}.`
        );
      }
    } else if (rev.eventId) {
      const exists = events.some((e) => e.eventId === rev.eventId);
      if (!exists) {
        throw new ArchiveIntegrityError(
          "bad-header",
          `Revision ${rev.revision} references unknown event ${rev.eventId}.`
        );
      }
    }
  }
  for (const correction of corrections) {
    if (
      correction.supersedes &&
      !corrections.some((c) => c.correctionId === correction.supersedes)
    ) {
      throw new ArchiveIntegrityError(
        "bad-header",
        `Correction ${correction.correctionId} supersedes unknown correction ${correction.supersedes}.`
      );
    }
    if (!leases.some((l) => l.leaseId === correction.leaseId)) {
      throw new ArchiveIntegrityError(
        "bad-header",
        `Correction ${correction.correctionId} references unknown lease ${correction.leaseId}.`
      );
    }
  }

  // Check destination conflicts for idempotency.
  const destEventCount = (
    db
      .prepare<[string], { c: number }>(
        `SELECT COUNT(*) AS c FROM events WHERE session_id = ?`
      )
      .get(targetSessionId) as { c: number }
  ).c;

  if (destEventCount > 0) {
    for (const event of events) {
      const row = db
        .prepare<[string, string], EventRow>(
          `SELECT event_id, source_id, source_seq, kind, text, created_at
           FROM events WHERE session_id = ? AND event_id = ?`
        )
        .get(targetSessionId, event.eventId);
      if (row) {
        if (
          row.source_id !== event.sourceId ||
          row.source_seq !== event.sourceSeq ||
          row.kind !== event.kind ||
          row.text !== event.text ||
          row.created_at !== event.createdAt
        ) {
          throw new ArchiveImportConflictError(
            "event",
            event.eventId,
            `Existing event ${event.eventId} differs from archive; refusing to overwrite.`
          );
        }
      }
    }
    for (const rev of revisions) {
      const row = db
        .prepare<[string, number], { snapshot: string; created_at: number }>(
          `SELECT snapshot, created_at FROM revisions
           WHERE session_id = ? AND revision = ?`
        )
        .get(targetSessionId, rev.revision);
      if (row) {
        const expectedSnapshot = stableStringify(rev.snapshot);
        const storedSnapshot = stableStringify(
          JSON.parse(row.snapshot) as unknown
        );
        if (row.created_at !== rev.createdAt || storedSnapshot !== expectedSnapshot) {
          throw new ArchiveImportConflictError(
            "revision",
            String(rev.revision),
            `Existing revision ${rev.revision} differs from archive; refusing to overwrite.`
          );
        }
      }
    }
  }

  const result: ArchiveImportResult = {
    sessionId: targetSessionId,
    imported: {
      events: 0,
      revisions: 0,
      corrections: 0,
      leases: 0,
      cursors: 0,
      unknown: 0,
    },
    duplicates: {
      events: 0,
      revisions: 0,
      corrections: 0,
      leases: 0,
      cursors: 0,
    },
    conflicting: 0,
  };

  const insertEvent = db.prepare(
    `INSERT OR IGNORE INTO events(
       session_id, event_id, source_id, source_seq, kind, text, created_at
     ) VALUES(?, ?, ?, ?, ?, ?, ?)`
  );
  const insertRevision = db.prepare(
    `INSERT OR IGNORE INTO revisions(
       session_id, revision, change_type, event_id, source_id, source_seq,
       kind, correction_id, snapshot, created_at
     ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );
  const insertCorrection = db.prepare(
    `INSERT OR IGNORE INTO corrections(
       session_id, correction_id, source_id, source_seq, actor, reason,
       original_text, corrected_text, base_revision, supersedes, lease_id, created_at
     ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );
  const upsertLease = db.prepare(
    `INSERT INTO review_leases(
       session_id, source_id, source_seq, lease_id, actor, base_revision,
       ttl_ms, claimed_at, expires_at, released_at
     ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(session_id, source_id, source_seq) DO UPDATE SET
       lease_id = excluded.lease_id,
       actor = excluded.actor,
       base_revision = excluded.base_revision,
       ttl_ms = excluded.ttl_ms,
       claimed_at = excluded.claimed_at,
       expires_at = excluded.expires_at,
       released_at = excluded.released_at`
  );
  const upsertCursor = db.prepare(
    `INSERT INTO consumer_cursors(session_id, consumer_id, cursor, updated_at)
     VALUES(?, ?, ?, ?)
     ON CONFLICT(session_id, consumer_id) DO UPDATE SET
       cursor = excluded.cursor,
       updated_at = excluded.updated_at`
  );
  const insertExtras = db.prepare(
    `INSERT OR REPLACE INTO archive_extras(session_id, record_table, record_key, extras)
     VALUES(?, ?, ?, ?)`
  );
  const insertUnknown = db.prepare(
    `INSERT OR REPLACE INTO archive_unknown_records(session_id, seq, record_type, payload)
     VALUES(?, ?, ?, ?)`
  );

  const apply = db.transaction(() => {
    for (const event of events) {
      const changes = insertEvent.run(
        targetSessionId,
        event.eventId,
        event.sourceId,
        event.sourceSeq,
        event.kind,
        event.text,
        event.createdAt
      ).changes;
      if (changes > 0) result.imported.events += 1;
      else result.duplicates.events += 1;
    }

    for (const rev of revisions) {
      const changes = insertRevision.run(
        targetSessionId,
        rev.revision,
        rev.changeType,
        rev.eventId ?? null,
        rev.sourceId ?? null,
        rev.sourceSeq ?? null,
        rev.kind ?? null,
        rev.correctionId ?? null,
        stableStringify(rev.snapshot),
        rev.createdAt
      ).changes;
      if (changes > 0) result.imported.revisions += 1;
      else result.duplicates.revisions += 1;
    }

    for (const correction of corrections) {
      const changes = insertCorrection.run(
        targetSessionId,
        correction.correctionId,
        correction.sourceId,
        correction.sourceSeq,
        correction.actor,
        correction.reason,
        correction.originalText,
        correction.correctedText,
        correction.baseRevision,
        correction.supersedes ?? null,
        correction.leaseId,
        correction.createdAt
      ).changes;
      if (changes > 0) result.imported.corrections += 1;
      else result.duplicates.corrections += 1;
    }

    for (const lease of leases) {
      const before = db
        .prepare<[string, string, number], { lease_id: string }>(
          `SELECT lease_id FROM review_leases
           WHERE session_id = ? AND source_id = ? AND source_seq = ?`
        )
        .get(targetSessionId, lease.sourceId, lease.sourceSeq);
      upsertLease.run(
        targetSessionId,
        lease.sourceId,
        lease.sourceSeq,
        lease.leaseId,
        lease.actor,
        lease.baseRevision,
        lease.ttlMs,
        lease.claimedAt,
        lease.expiresAt,
        lease.releasedAt ?? null
      );
      if (before) result.duplicates.leases += 1;
      else result.imported.leases += 1;
    }

    for (const cursor of cursors) {
      const before = db
        .prepare<[string, string], { cursor: number }>(
          `SELECT cursor FROM consumer_cursors
           WHERE session_id = ? AND consumer_id = ?`
        )
        .get(targetSessionId, cursor.consumerId);
      upsertCursor.run(
        targetSessionId,
        cursor.consumerId,
        cursor.cursor,
        cursor.updatedAt
      );
      if (before) result.duplicates.cursors += 1;
      else result.imported.cursors += 1;
    }

    for (const extra of extrasToWrite) {
      insertExtras.run(
        targetSessionId,
        extra.table,
        extra.key,
        stableStringify(extra.extras)
      );
    }

    for (const row of unknown) {
      insertUnknown.run(
        targetSessionId,
        row.seq,
        row.type,
        stableStringify(row.payload)
      );
      result.imported.unknown += 1;
    }
  });

  apply.immediate();
  return result;
}

export async function importSessionArchiveFile(
  db: Database.Database,
  targetSessionId: string,
  inputPath: string,
  options: ImportSessionArchiveOptions = {}
): Promise<ArchiveImportResult> {
  const stream = createReadStream(inputPath);
  return importSessionArchive(db, targetSessionId, stream, options);
}

export { ReviewConflictError };
