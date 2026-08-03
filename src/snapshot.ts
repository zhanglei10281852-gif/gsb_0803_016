import type Database from "better-sqlite3";
import type { CorrectionInfo, Segment, SegmentKind, Snapshot } from "./types";

interface SegmentRow {
  source_id: string;
  source_seq: number;
  kind: SegmentKind;
  event_text: string;
  corr_correction_id: string | null;
  corr_actor: string | null;
  corr_reason: string | null;
  corr_original_text: string | null;
  corr_corrected_text: string | null;
  corr_base_revision: number | null;
  corr_supersedes: string | null;
  corr_lease_id: string | null;
  corr_created_at: number | null;
}

interface StatsRow {
  event_count: number;
  source_count: number;
}

interface RevisionRow {
  revision: number;
}

const BASE_SEGMENTS_SQL = `
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
),
base_segments AS (
  SELECT source_id, source_seq, kind, text AS event_text FROM final_segments
  UNION ALL
  SELECT source_id, source_seq, kind, text AS event_text FROM active_partial
),
latest_correction AS (
  SELECT
    source_id,
    source_seq,
    correction_id,
    actor,
    reason,
    original_text,
    corrected_text,
    base_revision,
    supersedes,
    lease_id,
    created_at,
    ROW_NUMBER() OVER (
      PARTITION BY source_id, source_seq
      ORDER BY created_at DESC, correction_id DESC
    ) AS rn
  FROM corrections
  WHERE session_id = ?
)
SELECT
  b.source_id,
  b.source_seq,
  b.kind,
  b.event_text,
  c.correction_id AS corr_correction_id,
  c.actor AS corr_actor,
  c.reason AS corr_reason,
  c.original_text AS corr_original_text,
  c.corrected_text AS corr_corrected_text,
  c.base_revision AS corr_base_revision,
  c.supersedes AS corr_supersedes,
  c.lease_id AS corr_lease_id,
  c.created_at AS corr_created_at
FROM base_segments b
LEFT JOIN latest_correction c
  ON c.source_id = b.source_id
 AND c.source_seq = b.source_seq
 AND c.rn = 1
ORDER BY b.source_id, b.source_seq
`;

const STATS_SQL = `
SELECT
  COUNT(*) AS event_count,
  COUNT(DISTINCT source_id) AS source_count
FROM events
WHERE session_id = ?
`;

const MAX_REVISION_SQL = `
SELECT COALESCE(
  (SELECT MAX(revision) FROM revisions WHERE session_id = ?),
  (SELECT baseline_revision FROM compaction_state WHERE session_id = ?),
  0
) AS revision
`;

export interface SnapshotStatements {
  segments: Database.Statement<[string, string, string, string], SegmentRow>;
  stats: Database.Statement<[string], StatsRow>;
  maxRevision: Database.Statement<[string, string], RevisionRow>;
}

export function createSnapshotStatements(
  db: Database.Database,
): SnapshotStatements {
  return {
    segments: db.prepare<[string, string, string, string], SegmentRow>(
      BASE_SEGMENTS_SQL,
    ),
    stats: db.prepare<[string], StatsRow>(STATS_SQL),
    maxRevision: db.prepare<[string, string], RevisionRow>(MAX_REVISION_SQL),
  };
}

function toSegment(row: SegmentRow): Segment {
  let correction: CorrectionInfo | undefined;
  if (
    row.corr_correction_id &&
    row.corr_actor !== null &&
    row.corr_reason !== null &&
    row.corr_original_text !== null &&
    row.corr_corrected_text !== null &&
    row.corr_base_revision !== null &&
    row.corr_lease_id !== null &&
    row.corr_created_at !== null
  ) {
    correction = {
      correctionId: row.corr_correction_id,
      actor: row.corr_actor,
      reason: row.corr_reason,
      originalText: row.corr_original_text,
      correctedText: row.corr_corrected_text,
      baseRevision: row.corr_base_revision,
      ...(row.corr_supersedes === null
        ? {}
        : { supersedes: row.corr_supersedes }),
      leaseId: row.corr_lease_id,
      createdAt: row.corr_created_at,
    };
  }

  const correctedText = correction?.correctedText;
  return {
    sourceId: row.source_id,
    sourceSeq: row.source_seq,
    kind: row.kind,
    text: correctedText ?? row.event_text,
    originalText: row.event_text,
    ...(correction ? { correction } : {}),
  };
}

export function buildSnapshot(
  statements: SnapshotStatements,
  sessionId: string,
): Snapshot {
  const revisionRow = statements.maxRevision.get(sessionId, sessionId);
  const revision = revisionRow?.revision ?? 0;
  const stats = statements.stats.get(sessionId);
  const segmentRows = statements.segments.all(
    sessionId,
    sessionId,
    sessionId,
    sessionId,
  );

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
        correctedSegmentCount: 0,
        textLength: 0,
        finalTextLength: 0,
      },
    };
  }

  const segments = segmentRows.map(toSegment);
  const finalText = segments
    .filter((segment) => segment.kind === "final")
    .map((segment) => segment.text)
    .join("");
  const text = segments.map((segment) => segment.text).join("");
  const finalSegmentCount = segments.filter(
    (segment) => segment.kind === "final",
  ).length;
  const activePartialCount = segments.length - finalSegmentCount;
  const correctedSegmentCount = segments.filter(
    (segment) => segment.correction !== undefined,
  ).length;

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
      correctedSegmentCount,
      textLength: text.length,
      finalTextLength: finalText.length,
    },
  };
}
