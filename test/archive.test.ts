import Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import { join } from 'node:path';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { describe, expect, it } from 'vitest';
import { ConflictError, TranscriptStore, ValidationError } from '../src';
import type { AsrEvent } from '../src';
import { generateSession, makeDeliveries, mulberry32, tmpDbPath } from './helpers';

async function collectLines(store: TranscriptStore, sessionId: string): Promise<string[]> {
  const lines: string[] = [];
  for await (const line of store.exportArchive(sessionId)) lines.push(line.replace(/\n$/, ''));
  return lines;
}

function toText(lines: string[]): string {
  return lines.join('\n') + '\n';
}

/** Recompute the end marker so checksum passes; semantic validation remains. */
function rehash(lines: string[]): string[] {
  const payload = lines.slice(0, -1);
  const h = createHash('sha256');
  for (const l of payload) h.update(l + '\n');
  return [...payload, JSON.stringify({ type: 'end', records: payload.length - 1, sha256: h.digest('hex') })];
}

function editLine(lines: string[], pred: (r: Record<string, unknown>) => boolean, mutate: (r: Record<string, unknown>) => void): string[] {
  return lines.map((l) => {
    const r = JSON.parse(l) as Record<string, unknown>;
    if (pred(r)) mutate(r);
    return JSON.stringify(r);
  });
}

/** A source session with partials, finals, chained corrections and cursors. */
async function makeSource() {
  const store = new TranscriptStore(tmpDbPath('asr-arch-src-'));
  const bySource = generateSession(mulberry32(21), 2, 3);
  for (const d of makeDeliveries(mulberry32(22), bySource)) store.ingest('sess', d.sourceId, d.event);

  const fin = (n: number): AsrEvent => ({ eventId: `fx${n}`, sourceSeq: n, kind: 'final', text: `orig ${n}`, startMs: n * 100 });
  store.ingest('sess', 'mic-0', fin(100));
  store.ingest('sess', 'mic-0', fin(101));

  // two chained corrections on fx100 (carries actor/baseRevision/reason/supersedes)
  const t = { sourceId: 'mic-0', eventId: 'fx100' };
  let base = store.lastRevision('sess');
  const l1 = store.acquireLease('sess', t, { actor: 'alice', baseRevision: base, ttlMs: 60_000 });
  const c1 = store.submitCorrection('sess', t, { leaseId: l1.leaseId, baseRevision: base, text: 'fixed once', actor: 'alice', reason: 'typo' });
  const l2 = store.acquireLease('sess', t, { actor: 'bob', baseRevision: c1.revision, ttlMs: 60_000 });
  store.submitCorrection('sess', t, { leaseId: l2.leaseId, baseRevision: c1.revision, text: 'fixed twice', actor: 'bob', reason: 'style' });

  // cursors: one mid-stream, one fully acked
  base = store.lastRevision('sess');
  store.ack('qc', 'sess', base - 2);
  store.ack('done', 'sess', base);

  const snapshot = store.snapshot('sess');
  const stream = store.poll('audit', 'sess', 10000).entries;
  const cursors = { qc: store.cursor('qc', 'sess'), done: store.cursor('done', 'sess') };
  return { store, snapshot, stream, cursors };
}

describe('session archive export/import', () => {
  it('round-trip: snapshot, revision history and resume positions are equivalent', async () => {
    const src = await makeSource();
    const lines = await collectLines(src.store, 'sess');

    const dst = new TranscriptStore(tmpDbPath('asr-arch-dst-'));
    const result = await dst.importArchive(toText(lines));
    expect(result.status).toBe('imported');
    expect(result.lastRevision).toBe(src.snapshot.revision);

    expect(dst.snapshot('sess')).toEqual(src.snapshot);
    expect(dst.poll('audit', 'sess', 10000).entries).toEqual(src.stream);
    expect(dst.cursor('qc', 'sess')).toBe(src.cursors.qc);
    expect(dst.cursor('done', 'sess')).toBe(src.cursors.done);
    // acked prefixes are not redelivered on the imported copy
    expect(dst.poll('qc', 'sess', 10000).entries[0].revision).toBe(src.cursors.qc + 1);
    expect(dst.poll('done', 'sess', 10000).entries).toHaveLength(0);
    // ingest dedup memory and review stale-base survive the handoff
    const dup = dst.ingest('sess', 'mic-0', { eventId: 'fx100', sourceSeq: 100, kind: 'final', text: 'orig 100', startMs: 10000 });
    expect(dup.status).toBe('duplicate');
    src.store.close();
    dst.close();
  });

  it('re-import of the same archive is idempotent; a different archive for the session conflicts', async () => {
    const src = await makeSource();
    const text = toText(await collectLines(src.store, 'sess'));
    const dst = new TranscriptStore(tmpDbPath('asr-arch-dst-'));

    expect((await dst.importArchive(text)).status).toBe('imported');
    const snapAfterFirst = dst.snapshot('sess');
    const second = await dst.importArchive(text);
    expect(second.status).toBe('duplicate');
    expect(dst.snapshot('sess')).toEqual(snapAfterFirst);

    // a *different* archive for the same session must not merge
    const src2 = new TranscriptStore(tmpDbPath('asr-arch-src2-'));
    src2.ingest('sess', 'mic', { eventId: 'a', sourceSeq: 0, kind: 'final', text: 'other' });
    const other = toText(await collectLines(src2, 'sess'));
    await expect(dst.importArchive(other)).rejects.toThrow(ConflictError);
    expect(dst.snapshot('sess')).toEqual(snapAfterFirst);
    src.store.close();
    src2.close();
    dst.close();
  });

  it('file round-trip via exportArchiveToFile / importArchiveFromFile', async () => {
    const src = await makeSource();
    const dir = mkdtempSync(join(tmpdir(), 'asr-arch-file-'));
    const file = join(dir, 'sess.ndjson');
    await src.store.exportArchiveToFile('sess', file);
    const dst = new TranscriptStore(tmpDbPath('asr-arch-dst-'));
    const r = await dst.importArchiveFromFile(file);
    expect(r.status).toBe('imported');
    expect(dst.snapshot('sess')).toEqual(src.snapshot);
    src.store.close();
    dst.close();
  });

  it.each([
    ['truncated (no end marker)', (l: string[]) => l.slice(0, -1)],
    ['truncated mid-payload', (l: string[]) => [...l.slice(0, 3), ...l.slice(5)]],
    ['reordered payload', (l: string[]) => [l[0], l[2], l[1], ...l.slice(3)]],
  ])('%s fails before any state is visible', async (_name, mutate) => {
    const src = await makeSource();
    const bad = mutate(await collectLines(src.store, 'sess'));
    const dst = new TranscriptStore(tmpDbPath('asr-arch-dst-'));
    await expect(dst.importArchive(toText(bad))).rejects.toThrow(ValidationError);
    expect(dst.lastRevision('sess')).toBe(0);
    expect(dst.snapshot('sess').segments).toHaveLength(0);
    src.store.close();
    dst.close();
  });

  it('tampered payload with recomputed checksum fails semantic validation', async () => {
    const src = await makeSource();
    const lines = await collectLines(src.store, 'sess');

    const variants: string[][] = [
      // event text altered
      editLine(lines, (r) => r.type === 'event' && r.eventId === 'fx100', (r) => { r.text = 'forged'; }),
      // correction actor altered
      editLine(lines, (r) => r.type === 'correction', (r) => { r.actor = 'mallory'; }),
      // correction supersedes chain broken
      editLine(lines, (r) => r.type === 'correction' && r.supersedes !== null, (r) => { r.supersedes = 'nope'; }),
      // revision change text altered
      editLine(lines, (r) => r.type === 'revision' && (r.change as { type?: string }).type === 'final', (r) => { (r.change as { text: string }).text = 'forged'; }),
      // cursor beyond the stream
      editLine(lines, (r) => r.type === 'cursor', (r) => { r.ackedRevision = 99999; }),
      // revisions reordered (checksum recomputed, so only semantics can catch it)
      (() => {
        const idx = lines.findIndex((l) => JSON.parse(l).type === 'revision');
        const copy = [...lines];
        [copy[idx], copy[idx + 1]] = [copy[idx + 1], copy[idx]];
        return copy;
      })(),
    ];

    for (const variant of variants) {
      const dst = new TranscriptStore(tmpDbPath('asr-arch-dst-'));
      await expect(dst.importArchive(toText(rehash(variant)))).rejects.toThrow(ValidationError);
      expect(dst.lastRevision('sess')).toBe(0);
      dst.close();
    }
    src.store.close();
  });

  it('a stream that fails mid-way leaves nothing; the good archive then imports cleanly', async () => {
    const src = await makeSource();
    const lines = await collectLines(src.store, 'sess');
    const dst = new TranscriptStore(tmpDbPath('asr-arch-dst-'));

    async function* faulty(): AsyncGenerator<string> {
      for (const l of lines.slice(0, Math.floor(lines.length / 2))) yield l + '\n';
      throw new Error('connection reset');
    }
    await expect(dst.importArchive(faulty())).rejects.toThrow('connection reset');
    expect(dst.lastRevision('sess')).toBe(0);

    async function* good(): AsyncGenerator<string> {
      for (const l of lines) {
        yield l + '\n';
        await new Promise((r) => setTimeout(r, 0)); // slow stream
      }
    }
    const r = await dst.importArchive(good());
    expect(r.status).toBe('imported');
    expect(dst.snapshot('sess')).toEqual(src.snapshot);
    src.store.close();
    dst.close();
  });

  it('tolerates unknown optional fields and keeps the pristine copy; rejects newer versions', async () => {
    const src = await makeSource();
    const lines = await collectLines(src.store, 'sess');
    const enriched = editLine(lines, () => true, (r) => { r.futureField = { v: 42 }; });
    const dstFile = tmpDbPath('asr-arch-dst-');
    const dst = new TranscriptStore(dstFile);
    const r = await dst.importArchive(toText(rehash(enriched)));
    expect(r.status).toBe('imported');
    expect(dst.snapshot('sess')).toEqual(src.snapshot);
    dst.close();

    // pristine raw copy (with unknown fields) is retained in the imports table
    const raw = new Database(dstFile, { readonly: true });
    const row = raw.prepare('SELECT header_json, raw_json FROM imports WHERE session_id = ?').get('sess') as
      | { header_json: string; raw_json: string }
      | undefined;
    expect(row).toBeDefined();
    expect(row!.header_json).toContain('futureField');
    expect(row!.raw_json).toContain('futureField');
    raw.close();

    // version 2 archives are refused, even with a valid checksum
    const v2 = editLine(lines, (rec) => rec.type === 'header', (rec) => { rec.version = 2; });
    const dst2 = new TranscriptStore(tmpDbPath('asr-arch-dst-'));
    await expect(dst2.importArchive(toText(rehash(v2)))).rejects.toThrow(ValidationError);
    expect(dst2.lastRevision('sess')).toBe(0);
    dst2.close();
    src.store.close();
  });

  it('export of an unknown session fails cleanly', async () => {
    const s = new TranscriptStore(':memory:');
    await expect(collectLines(s, 'nope')).rejects.toThrow(ValidationError);
    s.close();
  });
});
