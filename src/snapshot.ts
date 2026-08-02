import type Database from "better-sqlite3";
import type { Snapshot, Segment, SegmentKind } from "./types";

interface SegmentRow {
  source_id: string;
  source_seq: number;
  kind: SegmentKind;
  text: string;
}

interface FinalTextRow {
  text: string;
}

interface StatsRow {
  event_count: number;
  source_count: number;
}

interface RevisionRow {
  revision: number;
}

const SEGMENTS_SQL = `
WITH final_segments AS (
  SELECT source_id, source_seq, kind, text
  FROM events
  WHERE session_id = ? AND kind = 'final'
),
latest_partial AS (
  SELECT source_id, MAX(source_seq) AS source_seq
  FROM events
  WHERE session_id = ? AND kind = 'partial'
  GROUP BY source_id
),
active_partial AS (
  SELECT e.source_id, e.source_seq, e.kind, e.text
  FROM events e
  JOIN latest_partial p
    ON p.source_id = e.source_id AND p.source_seq = e.source_seq
  LEFT JOIN (
    SELECT source_id, MAX(source_seq) AS final_seq
    FROM events
    WHERE session_id = ? AND kind = 'final'
    GROUP BY source_id
  ) f ON f.source_id = e.source_id
  WHERE e.kind = 'partial' AND e.source_seq > COALESCE(f.final_seq, -1)
)
SELECT source_id, source_seq, kind, text FROM final_segments
UNION ALL
SELECT source_id, source_seq, kind, text FROM active_partial
ORDER BY source_id, source_seq
`;

const FINAL_TEXT_SQL = `
SELECT text
FROM events
WHERE session_id = ? AND kind = 'final'
ORDER BY source_id, source_seq
`;

const STATS_SQL = `
SELECT
  COUNT(*) AS event_count,
  COUNT(DISTINCT source_id) AS source_count
FROM events
WHERE session_id = ?
`;

const MAX_REVISION_SQL = `
SELECT COALESCE(MAX(revision), 0) AS revision
FROM revisions
WHERE session_id = ?
`;

export interface SnapshotStatements {
  segments: Database.Statement<[string, string, string], SegmentRow>;
  finalSegments: Database.Statement<[string], FinalTextRow>;
  stats: Database.Statement<[string], StatsRow>;
  maxRevision: Database.Statement<[string], RevisionRow>;
}

export function createSnapshotStatements(
  db: Database.Database
): SnapshotStatements {
  return {
    segments: db.prepare<[string, string, string], SegmentRow>(SEGMENTS_SQL),
    finalSegments: db.prepare<[string], FinalTextRow>(FINAL_TEXT_SQL),
    stats: db.prepare<[string], StatsRow>(STATS_SQL),
    maxRevision: db.prepare<[string], RevisionRow>(MAX_REVISION_SQL),
  };
}

function toSegment(row: SegmentRow): Segment {
  return {
    sourceId: row.source_id,
    sourceSeq: row.source_seq,
    kind: row.kind,
    text: row.text,
  };
}

export function buildSnapshot(
  statements: SnapshotStatements,
  sessionId: string
): Snapshot {
  const revisionRow = statements.maxRevision.get(sessionId);
  const revision = revisionRow?.revision ?? 0;

  const segmentRows = statements.segments.all(sessionId, sessionId, sessionId);
  const finalRows = statements.finalSegments.all(sessionId);
  const stats = statements.stats.get(sessionId);

  if ((stats?.event_count ?? 0) === 0) {
    return {
      sessionId,
      revision: 0,
      segments: [],
      text: "",
      finalText: "",
      summary: {
        revision: 0,
        eventCount: 0,
        sourceCount: 0,
        finalSegmentCount: 0,
        activePartialCount: 0,
        textLength: 0,
        finalTextLength: 0,
      },
    };
  }

  const segments = segmentRows.map(toSegment);
  const finalText = finalRows.map((row) => row.text).join("");
  const text = segments.map((segment) => segment.text).join("");
  const finalSegmentCount = segments.filter(
    (segment) => segment.kind === "final"
  ).length;
  const activePartialCount = segments.length - finalSegmentCount;

  return {
    sessionId,
    revision,
    segments,
    text,
    finalText,
    summary: {
      revision,
      eventCount: stats?.event_count ?? 0,
      sourceCount: stats?.source_count ?? 0,
      finalSegmentCount,
      activePartialCount,
      textLength: text.length,
      finalTextLength: finalText.length,
    },
  };
}
