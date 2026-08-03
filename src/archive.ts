import { createHash, createHmac } from "node:crypto";

/**
 * Self-contained session archive format.
 *
 * Wire shape (newline-delimited JSON, one record per line, in this order):
 *
 *   1. header   — { type:"header", format, sessionId, createdAt, keyed }
 *   2. body     — session / event / segment / revision / correction / lease /
 *                 consumer records, in a deterministic canonical order
 *   3. trailer  — { type:"trailer", count, content, chain }
 *
 * Integrity is enforced by two digests carried in the trailer:
 *   - `chain`   — a running hash folded over EVERY line before the trailer
 *                 (header + body). Detects reordering, mutation, or truncation
 *                 of any line, because the fold is order-sensitive and the
 *                 trailer pins the final value.
 *   - `content` — a hash over the BODY lines only (header excluded, so the
 *                 volatile `createdAt` does not affect it). This is the stable
 *                 idempotency key: two exports of the same state produce the
 *                 same `content`.
 *
 * When a `secret` is supplied, both digests are HMACs keyed by it, so only a
 * holder of the secret can produce an archive that verifies on import.
 *
 * Forward compatibility: any object key a record carries that this version does
 * not recognise is preserved verbatim (see `KNOWN_FIELDS`) and re-emitted on
 * export, leaving room for later versions to add optional fields.
 */
export const ARCHIVE_FORMAT = 3;

/** Format versions this build can import. */
export const SUPPORTED_FORMATS: ReadonlySet<number> = new Set([3]);

export type ArchiveRecordType =
  | "header"
  | "session"
  | "event"
  | "segment"
  | "revision"
  | "correction"
  | "lease"
  | "consumer"
  | "trailer";

/** Recognised (non-ext) keys for each body record type. */
export const KNOWN_FIELDS: Record<string, ReadonlySet<string>> = {
  header: new Set(["type", "format", "sessionId", "createdAt", "keyed"]),
  session: new Set(["type", "sessionId", "headRevision"]),
  event: new Set([
    "type", "sessionId", "sourceId", "eventId", "segmentId", "sourceSeq",
    "kind", "text", "startMs", "endMs", "contentHash", "receivedAt",
  ]),
  segment: new Set([
    "type", "sessionId", "sourceId", "segmentId", "kind", "text", "startMs",
    "endMs", "sourceSeq", "eventId", "finalized", "revision", "origin", "actor",
  ]),
  revision: new Set([
    "type", "sessionId", "revision", "sourceId", "segmentId", "kind", "text",
    "startMs", "endMs", "eventId", "sourceSeq", "createdAt", "origin", "actor",
    "reason", "correctionId", "supersedesRevision",
  ]),
  correction: new Set([
    "type", "sessionId", "correctionId", "sourceId", "segmentId", "actor",
    "reason", "text", "baseRevision", "revision", "supersedesRevision",
    "leaseId", "createdAt",
  ]),
  lease: new Set([
    "type", "sessionId", "sourceId", "segmentId", "leaseId", "actor",
    "baseRevision", "acquiredAt", "expiresAt", "released",
  ]),
  consumer: new Set(["type", "sessionId", "consumerId", "cursor", "updatedAt"]),
  trailer: new Set(["type", "count", "content", "chain"]),
};

/**
 * Deterministic JSON: object keys sorted recursively so the byte output is
 * stable regardless of insertion order. Arrays keep their order. This is what
 * makes the content digest reproducible across exports.
 */
export function canonicalStringify(value: unknown): string {
  return JSON.stringify(sortValue(value));
}

function sortValue(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(sortValue);
  if (v && typeof v === "object") {
    const src = v as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(src).sort()) out[k] = sortValue(src[k]);
    return out;
  }
  return v;
}

/**
 * Split a parsed record into its recognised fields and its unknown ("ext")
 * fields, so importers on this version can persist and re-emit the unknowns.
 */
export function splitExt(
  type: string,
  obj: Record<string, unknown>,
): { known: Record<string, unknown>; ext: Record<string, unknown> } {
  const allow = KNOWN_FIELDS[type] ?? new Set<string>();
  const known: Record<string, unknown> = {};
  const ext: Record<string, unknown> = {};
  for (const [k, val] of Object.entries(obj)) {
    if (allow.has(k)) known[k] = val;
    else ext[k] = val;
  }
  return { known, ext };
}

/** Incremental integrity chain (plain sha256 or HMAC when keyed). */
export class Chain {
  private prev: string;
  private readonly secret: string | undefined;
  constructor(secret?: string) {
    this.secret = secret;
    this.prev = "";
  }
  /** Fold one raw line into the running digest. */
  update(line: string): void {
    const h = this.secret ? createHmac("sha256", this.secret) : createHash("sha256");
    h.update(this.prev);
    h.update("\n");
    h.update(line);
    this.prev = h.digest("hex");
  }
  digest(): string {
    return this.prev;
  }
}

/**
 * Incremental content digest over an ordered set of body lines. Streaming
 * friendly: fed one line at a time so neither export nor import needs to buffer
 * the whole session. Order-sensitive, but (unlike Chain) it does not include
 * the header, so the volatile header timestamp never perturbs the idempotency
 * key.
 */
export class Hasher {
  private readonly h;
  constructor(secret?: string) {
    this.h = secret ? createHmac("sha256", secret) : createHash("sha256");
  }
  update(line: string): void {
    this.h.update(line);
    this.h.update("\n");
  }
  digest(): string {
    return this.h.digest("hex");
  }
}

/** One-shot content digest over an ordered set of body lines. */
export function contentDigest(lines: string[], secret?: string): string {
  const hasher = new Hasher(secret);
  for (const line of lines) hasher.update(line);
  return hasher.digest();
}
