import type {
  FragmentRow,
  FragmentData,
  SessionSummary,
  SourceSnapshot,
} from './types';

export function rowsToFragments(rows: FragmentRow[]): FragmentData[] {
  return rows.map((r) => ({
    sourceId: r.source_id,
    sourceSeq: r.source_seq,
    type: r.event_type,
    content: r.content,
    corrected: r.is_corrected === 1,
  }));
}

export function buildText(fragments: FragmentData[]): string {
  let text = '';
  for (const f of fragments) {
    text += f.content;
  }
  return text;
}

export function buildSummary(fragments: FragmentData[]): SessionSummary {
  const sourceIds = new Set<string>();
  let finalCount = 0;
  let partialCount = 0;
  let correctedCount = 0;
  let textLength = 0;
  for (const f of fragments) {
    sourceIds.add(f.sourceId);
    if (f.type === 'final') {
      finalCount++;
    } else {
      partialCount++;
    }
    if (f.corrected) {
      correctedCount++;
    }
    textLength += f.content.length;
  }
  return {
    sourceCount: sourceIds.size,
    fragmentCount: fragments.length,
    finalCount,
    partialCount,
    correctedCount,
    textLength,
  };
}

export function groupBySource(
  fragments: FragmentData[],
): SourceSnapshot[] {
  const map = new Map<string, FragmentData[]>();
  for (const f of fragments) {
    let arr = map.get(f.sourceId);
    if (!arr) {
      arr = [];
      map.set(f.sourceId, arr);
    }
    arr.push(f);
  }
  const sourceIds = [...map.keys()].sort();
  return sourceIds.map((sourceId) => ({
    sourceId,
    fragments: map.get(sourceId)!,
  }));
}

export function buildSnapshotData(rows: FragmentRow[]): {
  fragments: FragmentData[];
  sources: SourceSnapshot[];
  text: string;
  summary: SessionSummary;
} {
  const fragments = rowsToFragments(rows);
  return {
    fragments,
    sources: groupBySource(fragments),
    text: buildText(fragments),
    summary: buildSummary(fragments),
  };
}
