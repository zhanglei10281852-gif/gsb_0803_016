import type Database from 'better-sqlite3';
import { createHash, randomUUID } from 'node:crypto';
import { ValidationError } from './errors';
import type { ChangeRecord } from './types';

/**
 * Self-contained session archive: NDJSON, one JSON record per line.
 *
 *   line 0:   { type:'header', format, version, archiveId, sessionId,
 *               exportedAt, lastRevision, counts:{...} }
 *   lines 1..n-2: { type:'event'|'revision'|'correction'|'cursor', ... }
 *   line n-1: { type:'end', records, sha256 }
 *
 * sha256 covers the raw text of every preceding line (each including its
 * trailing newline), so reordering, truncation or any byte-level tampering
 * is detected. On top of that the importer runs full semantic validation
 * (contiguity, cross-references), so a tampered archive fails even if its
 * checksum is recomputed. Unknown fields are tolerated (forward compat) and
 * preserved via the pristine raw copy stored in the imports table.
 */
export const ARCHIVE_FORMAT = 'asr-session-archive';
export const ARCHIVE_VERSION = 1;

export interface ArchiveEventRow {
  source_id: string;
  event_id: string;
  source_seq: number;
  kind: 'partial' | 'final';
  text: string;
  start_ms: number | null;
  content_hash: string;
  applied_revision: number | null;
}

export interface ArchiveRevisionRow {
  revision: number;
  change: ChangeRecord;
}

export interface ArchiveCorrectionRow {
  correction_id: string;
  target_source_id: string;
  target_event_id: string;
  text: string;
  actor: string;
  reason: string;
  supersedes: string | null;
  base_revision: number | null;
  revision: number;
}

export interface ArchiveCursorRow {
  consumer_id: string;
  acked_revision: number;
}

export interface ArchiveHeader {
  archiveId: string;
  sessionId: string;
  lastRevision: number;
}

export interface ParsedArchive {
  header: ArchiveHeader;
  headerJson: string;
  events: ArchiveEventRow[];
  revisions: ArchiveRevisionRow[];
  corrections: ArchiveCorrectionRow[];
  cursors: ArchiveCursorRow[];
  sha256: string;
  /** Normalized archive text (one record per line, trailing newline). */
  raw: string;
}

function hashLines(lines: string[]): string {
  const h = createHash('sha256');
  for (const line of lines) h.update(line + '\n');
  return h.digest('hex');
}

// ---------------------------------------------------------------- collect

/** Read the full session state in one consistent (transactional) pass. */
export function collectArchive(
  db: Database.Database,
  sessionId: string,
  now: () => number,
): string[] {
  const session = db
    .prepare('SELECT last_revision FROM sessions WHERE session_id = ?')
    .get(sessionId) as { last_revision: number } | undefined;
  if (!session) throw new ValidationError(`session "${sessionId}" does not exist`);

  const events = db
    .prepare(
      `SELECT source_id, event_id, source_seq, kind, text, start_ms, content_hash, applied_revision
       FROM events WHERE session_id = ? ORDER BY source_id, source_seq`,
    )
    .all(sessionId) as ArchiveEventRow[];
  const revisions = (
    db
      .prepare('SELECT revision, change_json FROM revisions WHERE session_id = ? ORDER BY revision')
      .all(sessionId) as { revision: number; change_json: string }[]
  ).map((r) => ({ revision: r.revision, change: JSON.parse(r.change_json) as ChangeRecord }));
  const corrections = db
    .prepare(
      `SELECT correction_id, target_source_id, target_event_id, text, actor, reason, supersedes, base_revision, revision
       FROM corrections WHERE session_id = ? ORDER BY revision`,
    )
    .all(sessionId) as ArchiveCorrectionRow[];
  const cursors = db
    .prepare(
      'SELECT consumer_id, acked_revision FROM cursors WHERE session_id = ? ORDER BY consumer_id',
    )
    .all(sessionId) as ArchiveCursorRow[];

  const lines: string[] = [];
  const header = {
    type: 'header',
    format: ARCHIVE_FORMAT,
    version: ARCHIVE_VERSION,
    archiveId: randomUUID(),
    sessionId,
    exportedAt: now(),
    lastRevision: session.last_revision,
    counts: {
      events: events.length,
      revisions: revisions.length,
      corrections: corrections.length,
      cursors: cursors.length,
    },
  };
  lines.push(JSON.stringify(header));
  for (const e of events) {
    lines.push(
      JSON.stringify({
        type: 'event',
        sourceId: e.source_id,
        eventId: e.event_id,
        sourceSeq: e.source_seq,
        kind: e.kind,
        text: e.text,
        startMs: e.start_ms,
        contentHash: e.content_hash,
        appliedRevision: e.applied_revision,
      }),
    );
  }
  for (const r of revisions) {
    lines.push(JSON.stringify({ type: 'revision', revision: r.revision, change: r.change }));
  }
  for (const c of corrections) {
    lines.push(
      JSON.stringify({
        type: 'correction',
        correctionId: c.correction_id,
        targetSourceId: c.target_source_id,
        targetEventId: c.target_event_id,
        text: c.text,
        actor: c.actor,
        reason: c.reason,
        supersedes: c.supersedes,
        baseRevision: c.base_revision,
        revision: c.revision,
      }),
    );
  }
  for (const c of cursors) {
    lines.push(
      JSON.stringify({ type: 'cursor', consumerId: c.consumer_id, ackedRevision: c.acked_revision }),
    );
  }
  lines.push(JSON.stringify({ type: 'end', records: lines.length - 1, sha256: hashLines(lines) }));
  return lines;
}

// ------------------------------------------------------------------ parse

function fail(message: string): never {
  throw new ValidationError(`invalid archive: ${message}`);
}

/** Internal map key for (sourceId, eventId) pairs. */
const pairKey = (sourceId: string, eventId: string) => sourceId + '\u0001' + eventId;

function asRecord(line: string, index: number): Record<string, unknown> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    fail(`line ${index + 1} is not valid JSON`);
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    fail(`line ${index + 1} is not a JSON object`);
  }
  return parsed as Record<string, unknown>;
}

function reqString(r: Record<string, unknown>, field: string, ctx: string): string {
  const v = r[field];
  if (typeof v !== 'string' || v.length === 0) fail(`${ctx}: "${field}" must be a non-empty string`);
  return v;
}

function reqInt(r: Record<string, unknown>, field: string, ctx: string, min = 0): number {
  const v = r[field];
  if (typeof v !== 'number' || !Number.isInteger(v) || v < min) {
    fail(`${ctx}: "${field}" must be an integer >= ${min}`);
  }
  return v;
}

function optIntOrNull(r: Record<string, unknown>, field: string, ctx: string): number | null {
  const v = r[field];
  if (v === null || v === undefined) return null;
  if (typeof v !== 'number' || !Number.isInteger(v)) fail(`${ctx}: "${field}" must be an integer or null`);
  return v;
}

function optStringOrNull(r: Record<string, unknown>, field: string, ctx: string): string | null {
  const v = r[field];
  if (v === null || v === undefined) return null;
  if (typeof v !== 'string') fail(`${ctx}: "${field}" must be a string or null`);
  return v;
}

/**
 * Parse and fully validate archive lines. Throws ValidationError on any
 * structural, integrity or semantic problem; nothing is applied before this
 * function returns successfully.
 */
export function parseArchive(lines: string[]): ParsedArchive {
  if (lines.length < 2) fail('archive has fewer than 2 lines (truncated)');
  const raw = lines.join('\n') + '\n';

  const header = asRecord(lines[0], 0);
  if (header.type !== 'header') fail('first record is not a header');
  if (header.format !== ARCHIVE_FORMAT) fail(`unexpected format "${String(header.format)}"`);
  const version = reqInt(header, 'version', 'header', 1);
  if (version > ARCHIVE_VERSION) {
    fail(`unsupported archive version ${version} (this build reads up to ${ARCHIVE_VERSION})`);
  }
  const archiveId = reqString(header, 'archiveId', 'header');
  const sessionId = reqString(header, 'sessionId', 'header');
  const lastRevision = reqInt(header, 'lastRevision', 'header');
  const counts = header.counts;
  if (counts === null || typeof counts !== 'object') fail('header.counts must be an object');

  const end = asRecord(lines[lines.length - 1], lines.length - 1);
  if (end.type !== 'end') fail('last record is not an end marker (truncated)');
  const payloadLines = lines.slice(1, -1);
  if (end.records !== payloadLines.length) {
    fail(`end marker records ${String(end.records)} but archive has ${payloadLines.length}`);
  }
  const sha256 = hashLines(lines.slice(0, -1));
  if (end.sha256 !== sha256) fail('checksum mismatch (reordered, truncated or tampered)');

  const events: ArchiveEventRow[] = [];
  const revisions: ArchiveRevisionRow[] = [];
  const corrections: ArchiveCorrectionRow[] = [];
  const cursors: ArchiveCursorRow[] = [];
  const eventKeys = new Set<string>();
  const partialSources = new Set<string>();

  payloadLines.forEach((line, i) => {
    const rec = asRecord(line, i + 1);
    const ctx = `record ${i + 2}`;
    switch (rec.type) {
      case 'event': {
        const e: ArchiveEventRow = {
          source_id: reqString(rec, 'sourceId', ctx),
          event_id: reqString(rec, 'eventId', ctx),
          source_seq: reqInt(rec, 'sourceSeq', ctx),
          kind: rec.kind === 'partial' || rec.kind === 'final' ? rec.kind : fail(`${ctx}: bad kind`),
          text: typeof rec.text === 'string' ? rec.text : (fail(`${ctx}: text must be a string`) as never),
          start_ms: optIntOrNull(rec, 'startMs', ctx),
          content_hash: reqString(rec, 'contentHash', ctx),
          applied_revision: optIntOrNull(rec, 'appliedRevision', ctx),
        };
        const key = pairKey(e.source_id, e.event_id);
        if (eventKeys.has(key)) fail(`${ctx}: duplicate event ${key}`);
        eventKeys.add(key);
        if (e.kind === 'partial') {
          if (partialSources.has(e.source_id)) fail(`${ctx}: more than one partial for source ${e.source_id}`);
          partialSources.add(e.source_id);
        }
        events.push(e);
        break;
      }
      case 'revision': {
        revisions.push({
          revision: reqInt(rec, 'revision', ctx, 1),
          change: rec.change as ChangeRecord,
        });
        if (rec.change === null || typeof rec.change !== 'object') fail(`${ctx}: change must be an object`);
        break;
      }
      case 'correction': {
        corrections.push({
          correction_id: reqString(rec, 'correctionId', ctx),
          target_source_id: reqString(rec, 'targetSourceId', ctx),
          target_event_id: reqString(rec, 'targetEventId', ctx),
          text: typeof rec.text === 'string' ? rec.text : (fail(`${ctx}: text must be a string`) as never),
          actor: reqString(rec, 'actor', ctx),
          reason: typeof rec.reason === 'string' ? rec.reason : (fail(`${ctx}: reason must be a string`) as never),
          supersedes: optStringOrNull(rec, 'supersedes', ctx),
          base_revision: optIntOrNull(rec, 'baseRevision', ctx),
          revision: reqInt(rec, 'revision', ctx, 1),
        });
        break;
      }
      case 'cursor': {
        cursors.push({
          consumer_id: reqString(rec, 'consumerId', ctx),
          acked_revision: reqInt(rec, 'ackedRevision', ctx),
        });
        break;
      }
      default:
        fail(`${ctx}: unknown record type "${String(rec.type)}"`);
    }
  });

  // counts in the header must match reality
  const want = counts as Record<string, unknown>;
  if (want.events !== events.length || want.revisions !== revisions.length ||
      want.corrections !== corrections.length || want.cursors !== cursors.length) {
    fail('header.counts do not match the payload');
  }

  // revisions must be exactly 1..lastRevision, in order
  if (revisions.length !== lastRevision) {
    fail(`expected ${lastRevision} revision records, found ${revisions.length}`);
  }
  revisions.forEach((r, i) => {
    if (r.revision !== i + 1) fail(`revisions are not contiguous and ordered (record ${i + 2})`);
  });

  const eventByKey = new Map(events.map((e) => [pairKey(e.source_id, e.event_id), e]));
  const corrById = new Map(corrections.map((c) => [c.correction_id, c]));
  const eq = (a: unknown, b: unknown) => (a ?? null) === (b ?? null);

  // every correction must reference an existing target and chain correctly
  for (const c of corrections) {
    const target = eventByKey.get(pairKey(c.target_source_id, c.target_event_id));
    if (!target) fail(`correction ${c.correction_id} targets a missing event`);
    if (target.kind !== 'final') fail(`correction ${c.correction_id} targets a non-final event`);
    if (c.supersedes !== null) {
      const prev = corrById.get(c.supersedes);
      if (!prev) fail(`correction ${c.correction_id} supersedes a missing correction`);
      if (prev.revision >= c.revision) fail(`correction ${c.correction_id} supersedes chain is not ordered`);
    }
    if (!revisions.some((r) => r.revision === c.revision && r.change?.type === 'correction')) {
      fail(`correction ${c.correction_id} has no matching revision record`);
    }
  }

  // every revision change must match its state record exactly
  for (const r of revisions) {
    const ch = r.change;
    const ctx = `revision ${r.revision}`;
    if (ch.type === 'partial' || ch.type === 'final') {
      const e = eventByKey.get(pairKey(String(ch.sourceId), String(ch.eventId)));
      if (!e) {
        // Superseded partials are deleted from current state but stay in the
        // revision history; finals are never deleted, so a missing one is invalid.
        if (ch.type === 'final') fail(`${ctx}: references a missing final event`);
        continue;
      }
      if (e.kind !== ch.type || e.source_seq !== ch.sourceSeq || e.text !== ch.text ||
          !eq(e.start_ms, ch.startMs)) {
        fail(`${ctx}: change does not match its event record`);
      }
      if (!eq(e.applied_revision, r.revision)) fail(`${ctx}: event applied_revision mismatch`);
    } else if (ch.type === 'correction') {
      const c = corrById.get(String(ch.correctionId));
      if (!c) fail(`${ctx}: references a missing correction`);
      if (c.revision !== r.revision || c.text !== ch.text || c.actor !== ch.actor ||
          c.reason !== ch.reason || !eq(c.supersedes, ch.supersedes) ||
          !eq(c.base_revision, ch.baseRevision) ||
          c.target_source_id !== ch.sourceId || c.target_event_id !== ch.eventId) {
        fail(`${ctx}: change does not match its correction record`);
      }
    } else {
      fail(`${ctx}: unknown change type "${String(ch?.type)}"`);
    }
  }

  // cursors must be within the stream bounds
  for (const c of cursors) {
    if (c.acked_revision > lastRevision) {
      fail(`cursor "${c.consumer_id}" acked ${c.acked_revision} beyond lastRevision ${lastRevision}`);
    }
  }

  return {
    header: { archiveId, sessionId, lastRevision },
    headerJson: lines[0],
    events,
    revisions,
    corrections,
    cursors,
    sha256,
    raw,
  };
}
