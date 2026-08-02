import { createHash } from "node:crypto";
import type { SourceEvent } from "./types";

/**
 * Canonical content fingerprint for an event.
 *
 * Two deliveries of the same `eventId` are considered identical iff this hash
 * matches. It intentionally excludes `sessionId`/`sourceId`/`eventId` (those
 * are the identity, already keyed in storage) and includes only the semantic
 * payload that a re-delivery must preserve.
 *
 * The encoding is length-prefixed to avoid delimiter-injection ambiguity
 * (e.g. text containing separators), so the hash is stable and collision-safe
 * for distinct field combinations.
 */
export function contentHash(ev: SourceEvent): string {
  const h = createHash("sha256");
  const parts: Array<string | number> = [
    ev.segmentId,
    ev.sourceSeq,
    ev.kind,
    ev.text,
    ev.startMs ?? "",
    ev.endMs ?? "",
  ];
  for (const p of parts) {
    const s = String(p);
    h.update(String(s.length));
    h.update("\u0000");
    h.update(s);
    h.update("\u0000");
  }
  return h.digest("hex");
}
