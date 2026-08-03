import { describe, expect, it } from 'vitest';
import { TranscriptStore } from '../src';
import type { AsrEvent } from '../src';

const fin = (seq: number, text: string): AsrEvent => ({
  eventId: `e${seq}`,
  sourceSeq: seq,
  kind: 'final',
  text,
  startMs: seq * 10,
});

function feed(s: TranscriptStore, n: number): void {
  for (let i = 0; i < n; i++) s.ingest('sess', 'mic', fin(i, `seg ${i}`));
}

describe('revision stream & consumer cursors', () => {
  it('revisions are contiguous from 1 and strictly increasing', () => {
    const s = new TranscriptStore(':memory:');
    feed(s, 20);
    const { entries, lastRevision } = s.poll('c', 'sess', 1000);
    expect(lastRevision).toBe(20);
    expect(entries.map((e) => e.revision)).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
    s.close();
  });

  it('never resends acked revisions', () => {
    const s = new TranscriptStore(':memory:');
    feed(s, 10);
    s.poll('c', 'sess', 4);
    s.ack('c', 'sess', 4);
    const { entries, ackedRevision } = s.poll('c', 'sess', 100);
    expect(ackedRevision).toBe(4);
    expect(entries[0].revision).toBe(5);
    s.close();
  });

  it('redelivers unacked revisions (at-least-once) until acked', () => {
    const s = new TranscriptStore(':memory:');
    feed(s, 5);
    const first = s.poll('c', 'sess', 3);
    const again = s.poll('c', 'sess', 3); // no ack in between
    expect(again.entries.map((e) => e.revision)).toEqual(first.entries.map((e) => e.revision));
    s.ack('c', 'sess', 3);
    expect(s.poll('c', 'sess', 10).entries.map((e) => e.revision)).toEqual([4, 5]);
    s.close();
  });

  it('ack is monotonic and clamped to the last revision', () => {
    const s = new TranscriptStore(':memory:');
    feed(s, 5);
    expect(s.ack('c', 'sess', 99)).toBe(5);
    expect(s.ack('c', 'sess', 2)).toBe(5); // older ack is a no-op
    expect(s.cursor('c', 'sess')).toBe(5);
    expect(s.poll('c', 'sess', 10).entries).toHaveLength(0);
    s.close();
  });

  it('cursors are per consumer and per session', () => {
    const s = new TranscriptStore(':memory:');
    feed(s, 3);
    s.ingest('other', 'mic', fin(0, 'x'));
    s.ack('fast', 'sess', 3);
    expect(s.poll('fast', 'sess', 10).entries).toHaveLength(0);
    expect(s.poll('slow', 'sess', 10).entries).toHaveLength(3);
    expect(s.poll('fast', 'other', 10).entries).toHaveLength(1);
    s.close();
  });

  it('empty session polls return nothing', () => {
    const s = new TranscriptStore(':memory:');
    expect(s.poll('c', 'nope', 10)).toEqual({ entries: [], ackedRevision: 0, lastRevision: 0 });
    s.close();
  });
});
