import { buildSnapshot, eventHash, type EventRow } from './core';
import { ConflictError } from './errors';
import type { AsrEvent, ChangeRecord, Snapshot } from './types';

interface StoredEvent extends EventRow {
  content_hash: string;
}

/**
 * Pure in-memory reimplementation of the ingest rules, applying events in
 * whatever order it is fed. Tests and e2e feed it events in canonical
 * (per-source sourceSeq) order and assert the SQLite store converges to the
 * same snapshot under arbitrary interleavings.
 */
export class ReferenceModel {
  private readonly byKey = new Map<string, StoredEvent>();
  private readonly maxPartial = new Map<string, number>();
  private readonly maxFinal = new Map<string, number>();
  /** Change records of every applied ingest, in application order. */
  readonly applied: ChangeRecord[] = [];

  ingest(sourceId: string, event: AsrEvent): 'applied' | 'duplicate' | 'stale' {
    const key = `${sourceId}${event.eventId}`;
    const hash = eventHash(event);
    const existing = this.byKey.get(key);
    if (existing) {
      if (existing.content_hash === hash) return 'duplicate';
      throw new ConflictError(
        `event "${event.eventId}" from source "${sourceId}" already exists with different content`,
      );
    }
    const maxPartial = this.maxPartial.get(sourceId) ?? -1;
    const maxFinal = this.maxFinal.get(sourceId) ?? -1;

    if (event.kind === 'partial') {
      if (event.sourceSeq <= maxPartial || event.sourceSeq <= maxFinal) return 'stale';
      for (const [k, v] of this.byKey) {
        if (v.source_id === sourceId && v.kind === 'partial') this.byKey.delete(k);
      }
      this.maxPartial.set(sourceId, event.sourceSeq);
    } else {
      for (const [k, v] of this.byKey) {
        if (v.source_id === sourceId && v.kind === 'partial' && v.source_seq <= event.sourceSeq) {
          this.byKey.delete(k);
        }
      }
      this.maxFinal.set(sourceId, Math.max(maxFinal, event.sourceSeq));
    }
    this.byKey.set(key, {
      source_id: sourceId,
      event_id: event.eventId,
      source_seq: event.sourceSeq,
      kind: event.kind,
      text: event.text,
      start_ms: event.startMs ?? null,
      content_hash: hash,
    });
    this.applied.push({
      type: event.kind,
      sourceId,
      eventId: event.eventId,
      sourceSeq: event.sourceSeq,
      text: event.text,
      startMs: event.startMs ?? null,
    });
    return 'applied';
  }

  snapshot(sessionId = '<reference>'): Snapshot {
    const rows: EventRow[] = [...this.byKey.values()].map((e) => ({
      source_id: e.source_id,
      event_id: e.event_id,
      source_seq: e.source_seq,
      kind: e.kind,
      text: e.text,
      start_ms: e.start_ms,
    }));
    return buildSnapshot(sessionId, this.applied.length, rows);
  }
}
