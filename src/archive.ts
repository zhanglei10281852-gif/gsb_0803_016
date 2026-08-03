import { createHash } from 'node:crypto';
import {
  createWriteStream,
  readFileSync,
} from 'node:fs';
import { Readable } from 'node:stream';
import type { Database as DatabaseType } from 'better-sqlite3';
import {
  ArchiveFormatError,
  ArchiveChecksumError,
  ArchiveVersionError,
  SessionExistsError,
} from './errors';

const ARCHIVE_MAGIC = Buffer.from('ASRA', 'ascii');
const ARCHIVE_VERSION = 1;
const SEGMENT_HEADER_SIZE = 5;

type SegmentType =
  | 'H' | 'S' | 'E' | 'F' | 'R' | 'C' | 'L' | 'U' | 'K' | 'X';

export interface ArchiveCounts {
  session: number;
  events: number;
  fragments: number;
  revisions: number;
  corrections: number;
  leases: number;
  cursors: number;
  checkpoints: number;
}

interface ArchiveHeader {
  sessionId: string;
  exportedAt: number;
  formatVersion: number;
  counts: ArchiveCounts;
}

interface ParsedArchive {
  header: ArchiveHeader;
  sessionRow: Record<string, unknown>;
  events: Record<string, unknown>[];
  fragments: Record<string, unknown>[];
  revisions: Record<string, unknown>[];
  corrections: Record<string, unknown>[];
  leases: Record<string, unknown>[];
  cursors: Record<string, unknown>[];
  checkpoints: Record<string, unknown>[];
  unknownSegments: { type: string; payload: Buffer }[];
}

export interface ExportResult {
  sessionId: string;
  path: string;
  bytesWritten: number;
  counts: ArchiveCounts;
  sha256: string;
  maxRevision: number;
}

export interface ImportResult {
  sessionId: string;
  idempotent: boolean;
  counts: ArchiveCounts;
}

export function createSessionArchive(
  db: DatabaseType,
  sessionId: string,
): AsyncIterable<Buffer> {
  return generateArchive(db, sessionId);
}

export function writeSessionArchive(
  db: DatabaseType,
  sessionId: string,
  filePath: string,
): Promise<ExportResult> {
  return new Promise((resolve, reject) => {
    const writer = createWriteStream(filePath);
    let bytesWritten = 0;
    let counts: ArchiveCounts | null = null;
    let maxRevision = 0;
    const fileHasher = createHash('sha256');

    writer.on('error', reject);
    writer.on('finish', () => {
      if (counts) {
        resolve({
          sessionId,
          path: filePath,
          bytesWritten,
          counts,
          sha256: fileHasher.digest('hex'),
          maxRevision,
        });
      }
    });

    (async () => {
      try {
        for await (const chunk of generateArchive(db, sessionId)) {
          if (chunk.length === 0) continue;
          fileHasher.update(chunk);
          if (chunk[0] === 0x48) {
            const len = chunk.readUInt32LE(1);
            const json = JSON.parse(
              chunk
                .slice(SEGMENT_HEADER_SIZE, SEGMENT_HEADER_SIZE + len)
                .toString('utf8'),
            ) as ArchiveHeader;
            counts = json.counts;
          }
          bytesWritten += chunk.length;
          writer.write(chunk);
        }
        const revRow = db
          .prepare(
            'SELECT latest_revision AS r FROM sessions WHERE session_id = ?',
          )
          .get(sessionId) as { r: number } | undefined;
        maxRevision = revRow ? revRow.r : 0;
        writer.end();
      } catch (err) {
        writer.destroy(err as Error);
      }
    })();
  });
}

async function* generateArchive(
  db: DatabaseType,
  sessionId: string,
): AsyncGenerator<Buffer> {
  const sessionRow = db
    .prepare(
      'SELECT session_id, created_at, updated_at, latest_revision FROM sessions WHERE session_id = ?',
    )
    .get(sessionId) as
    | {
        session_id: string;
        created_at: number;
        updated_at: number;
        latest_revision: number;
      }
    | undefined;

  if (!sessionRow) {
    throw new ArchiveFormatError(
      `session '${sessionId}' not found for export`,
    );
  }

  const countRow = db
    .prepare(
      `SELECT
        (SELECT COUNT(*) FROM incoming_events WHERE session_id = ?) AS events,
        (SELECT COUNT(*) FROM fragments WHERE session_id = ?) AS fragments,
        (SELECT COUNT(*) FROM revisions WHERE session_id = ?) AS revisions,
        (SELECT COUNT(*) FROM corrections WHERE session_id = ?) AS corrections,
        (SELECT COUNT(*) FROM correction_leases WHERE session_id = ?) AS leases,
        (SELECT COUNT(*) FROM consumer_cursors WHERE session_id = ?) AS cursors,
        (SELECT COUNT(*) FROM compaction_checkpoints WHERE session_id = ?) AS checkpoints`,
    )
    .get(
      sessionId,
      sessionId,
      sessionId,
      sessionId,
      sessionId,
      sessionId,
      sessionId,
    ) as Record<string, number>;

  const counts: ArchiveCounts = {
    session: 1,
    events: countRow.events,
    fragments: countRow.fragments,
    revisions: countRow.revisions,
    corrections: countRow.corrections,
    leases: countRow.leases,
    cursors: countRow.cursors,
    checkpoints: countRow.checkpoints,
  };

  const hasher = createHash('sha256');

  yield ARCHIVE_MAGIC;
  hasher.update(ARCHIVE_MAGIC);

  const versionBuf = Buffer.alloc(4);
  versionBuf.writeUInt32LE(ARCHIVE_VERSION, 0);
  yield versionBuf;
  hasher.update(versionBuf);

  const header: ArchiveHeader = {
    sessionId,
    exportedAt: Date.now(),
    formatVersion: ARCHIVE_VERSION,
    counts,
  };

  const headerSeg = encodeSegment('H', JSON.stringify(header));
  yield headerSeg;
  hasher.update(headerSeg);

  const sessionSeg = encodeSegment('S', JSON.stringify(sessionRow));
  yield sessionSeg;
  hasher.update(sessionSeg);

  const tableDefs: ReadonlyArray<readonly [SegmentType, string, string]> = [
    ['E', 'incoming_events', 'event_id'],
    ['F', 'fragments', 'source_id, source_seq'],
    ['R', 'revisions', 'revision'],
    ['C', 'corrections', 'revision'],
    ['L', 'correction_leases', 'source_id, source_seq'],
    ['U', 'consumer_cursors', 'consumer_id'],
    ['K', 'compaction_checkpoints', 'max_revision'],
  ];

  for (const [type, table, orderBy] of tableDefs) {
    const rows = db
      .prepare(
        `SELECT * FROM ${table} WHERE session_id = ? ORDER BY ${orderBy}`,
      )
      .all(sessionId) as Record<string, unknown>[];

    for (const row of rows) {
      const seg = encodeSegment(type, JSON.stringify(row));
      yield seg;
      hasher.update(seg);
    }
  }

  const checksum = hasher.digest('hex');
  const trailer = encodeSegment(
    'X',
    JSON.stringify({ sha256: checksum, counts }),
  );
  yield trailer;
}

function encodeSegment(type: SegmentType, json: string): Buffer {
  const payload = Buffer.from(json, 'utf8');
  const buf = Buffer.alloc(SEGMENT_HEADER_SIZE + payload.length);
  buf.write(type, 0, 'ascii');
  buf.writeUInt32LE(payload.length, 1);
  payload.copy(buf, SEGMENT_HEADER_SIZE);
  return buf;
}

export function parseArchiveBuffer(buf: Buffer): ParsedArchive {
  if (buf.length < 8) {
    throw new ArchiveFormatError('archive too short: missing header');
  }
  if (!buf.subarray(0, 4).equals(ARCHIVE_MAGIC)) {
    throw new ArchiveFormatError('invalid archive magic bytes');
  }

  const version = buf.readUInt32LE(4);
  if (version > ARCHIVE_VERSION) {
    throw new ArchiveVersionError(
      `archive format version ${version} is newer than supported version ${ARCHIVE_VERSION}`,
    );
  }

  const hasher = createHash('sha256');
  hasher.update(buf.subarray(0, 8));

  let offset = 8;
  let header: ArchiveHeader | null = null;
  let sessionRow: Record<string, unknown> | null = null;
  const events: Record<string, unknown>[] = [];
  const fragments: Record<string, unknown>[] = [];
  const revisions: Record<string, unknown>[] = [];
  const corrections: Record<string, unknown>[] = [];
  const leases: Record<string, unknown>[] = [];
  const cursors: Record<string, unknown>[] = [];
  const checkpoints: Record<string, unknown>[] = [];
  const unknownSegments: { type: string; payload: Buffer }[] = [];
  let checksumSegment: { sha256: string; counts: ArchiveCounts } | null =
    null;

  while (offset < buf.length) {
    if (offset + SEGMENT_HEADER_SIZE > buf.length) {
      throw new ArchiveFormatError(
        `truncated segment header at offset ${offset}`,
      );
    }
    const type = buf.toString(
      'ascii',
      offset,
      offset + 1,
    ) as SegmentType;
    const length = buf.readUInt32LE(offset + 1);
    const payloadStart = offset + SEGMENT_HEADER_SIZE;
    const payloadEnd = payloadStart + length;

    if (payloadEnd > buf.length) {
      throw new ArchiveFormatError(
        `truncated segment payload at offset ${offset} (declared ${length} bytes, available ${buf.length - payloadStart})`,
      );
    }

    const segmentBytes = buf.subarray(offset, payloadEnd);
    const payloadBuf = buf.subarray(payloadStart, payloadEnd);

    if (type === 'X') {
      checksumSegment = JSON.parse(payloadBuf.toString('utf8')) as {
        sha256: string;
        counts: ArchiveCounts;
      };
    } else {
      hasher.update(segmentBytes);

      const jsonStr = payloadBuf.toString('utf8');
      const obj = JSON.parse(jsonStr) as Record<string, unknown>;

      switch (type) {
        case 'H':
          header = obj as unknown as ArchiveHeader;
          break;
        case 'S':
          sessionRow = obj;
          break;
        case 'E':
          events.push(obj);
          break;
        case 'F':
          fragments.push(obj);
          break;
        case 'R':
          revisions.push(obj);
          break;
        case 'C':
          corrections.push(obj);
          break;
        case 'L':
          leases.push(obj);
          break;
        case 'U':
          cursors.push(obj);
          break;
        case 'K':
          checkpoints.push(obj);
          break;
        default:
          unknownSegments.push({
            type,
            payload: Buffer.from(payloadBuf),
          });
      }
    }

    offset = payloadEnd;
  }

  if (!checksumSegment) {
    throw new ArchiveFormatError('archive missing checksum trailer');
  }
  if (!header) {
    throw new ArchiveFormatError('archive missing header segment');
  }
  if (!sessionRow) {
    throw new ArchiveFormatError('archive missing session segment');
  }

  const computedChecksum = hasher.digest('hex');
  if (computedChecksum !== checksumSegment.sha256) {
    throw new ArchiveChecksumError(
      `archive checksum mismatch: expected ${checksumSegment.sha256}, computed ${computedChecksum}`,
    );
  }

  const actualCounts: ArchiveCounts = {
    session: 1,
    events: events.length,
    fragments: fragments.length,
    revisions: revisions.length,
    corrections: corrections.length,
    leases: leases.length,
    cursors: cursors.length,
    checkpoints: checkpoints.length,
  };

  assertCountsMatch(header.counts, actualCounts, 'header');
  assertCountsMatch(checksumSegment.counts, actualCounts, 'trailer');

  return {
    header,
    sessionRow,
    events,
    fragments,
    revisions,
    corrections,
    leases,
    cursors,
    checkpoints,
    unknownSegments,
  };
}

function assertCountsMatch(
  expected: ArchiveCounts,
  actual: ArchiveCounts,
  label: string,
): void {
  for (const key of Object.keys(expected) as (keyof ArchiveCounts)[]) {
    if (actual[key] !== expected[key]) {
      throw new ArchiveFormatError(
        `archive count mismatch in ${label} for ${key}: expected ${expected[key]}, found ${actual[key]}`,
      );
    }
  }
}

export async function parseArchiveFromStream(
  readable: Readable,
): Promise<ParsedArchive> {
  const chunks: Buffer[] = [];
  for await (const chunk of readable) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return parseArchiveBuffer(Buffer.concat(chunks));
}

export function readSessionArchive(filePath: string): ParsedArchive {
  return parseArchiveBuffer(readFileSync(filePath));
}

export function importSessionArchive(
  db: DatabaseType,
  archivePath: string,
): ImportResult {
  const buf = readFileSync(archivePath);
  const parsed = parseArchiveBuffer(buf);

  const sessionId = parsed.header.sessionId;
  const counts = parsed.header.counts;

  const existing = db
    .prepare('SELECT session_id FROM sessions WHERE session_id = ?')
    .get(sessionId) as { session_id: string } | undefined;

  if (existing) {
    if (verifySessionIdentical(db, parsed)) {
      return { sessionId, idempotent: true, counts };
    }
    throw new SessionExistsError(
      `session '${sessionId}' already exists with different content`,
    );
  }

  const importTxn = db.transaction(() => {
    insertRow(db, 'sessions', parsed.sessionRow);
    for (const row of parsed.events)
      insertRow(db, 'incoming_events', row);
    for (const row of parsed.fragments) insertRow(db, 'fragments', row);
    for (const row of parsed.revisions) insertRow(db, 'revisions', row);
    for (const row of parsed.corrections)
      insertRow(db, 'corrections', row);
    for (const row of parsed.leases)
      insertRow(db, 'correction_leases', row);
    for (const row of parsed.cursors)
      insertRow(db, 'consumer_cursors', row);
    for (const row of parsed.checkpoints)
      insertRow(db, 'compaction_checkpoints', row);
  });

  importTxn.immediate();

  return { sessionId, idempotent: false, counts };
}

function verifySessionIdentical(
  db: DatabaseType,
  parsed: ParsedArchive,
): boolean {
  const tables: [string, Record<string, unknown>[]][] = [
    ['incoming_events', parsed.events],
    ['fragments', parsed.fragments],
    ['revisions', parsed.revisions],
    ['corrections', parsed.corrections],
    ['correction_leases', parsed.leases],
    ['consumer_cursors', parsed.cursors],
    ['compaction_checkpoints', parsed.checkpoints],
  ];

  for (const [table, expectedRows] of tables) {
    const countRow = db
      .prepare(`SELECT COUNT(*) AS c FROM ${table} WHERE session_id = ?`)
      .get(parsed.header.sessionId) as { c: number };
    if (countRow.c !== expectedRows.length) return false;
  }

  const sessionRow = db
    .prepare(
      'SELECT latest_revision, created_at FROM sessions WHERE session_id = ?',
    )
    .get(parsed.header.sessionId) as {
    latest_revision: number;
    created_at: number;
  };
  const expectedSession = parsed.sessionRow as Record<string, unknown>;
  if (sessionRow.latest_revision !== expectedSession.latest_revision) {
    return false;
  }
  if (sessionRow.created_at !== expectedSession.created_at) {
    return false;
  }

  return true;
}

function insertRow(
  db: DatabaseType,
  table: string,
  row: Record<string, unknown>,
): void {
  const keys = Object.keys(row);
  const placeholders = keys.map(() => '?').join(', ');
  const columns = keys.join(', ');
  db.prepare(
    `INSERT OR IGNORE INTO ${table} (${columns}) VALUES (${placeholders})`,
  ).run(...keys.map((k) => row[k]));
}
