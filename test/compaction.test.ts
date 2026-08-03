import { describe, expect, it } from 'vitest';
import { ReferenceModel, ResetRequiredError, TranscriptStore } from '../src';
import type { AsrEvent } from '../src';
import { generateSession, makeDeliveries, mulberry32, tmpDbPath } from './helpers';

const fin = (seq: number, text: string, id = `e${seq}`): AsrEvent => ({
  eventId: id,
  sourceSeq: seq,
  kind: 'final',
  text,
  startMs: seq * 10,
});

function feed(s: TranscriptStore, n: number, base = 0): void {
  for (let i = base; i < base + n; i++) s.ingest('sess', 'mic', fin(i, `seg ${i}`));
}

async function collectLines(store: TranscriptStore, sessionId: string): Promise<string[]> {
  const lines: string[] = [];
  for await (const line of store.exportArchive(sessionId)) lines.push(line.replace(/\n$/, ''));
  return lines;
}

describe('compaction', () => {
  it('reclaims only history at or below the slowest live cursor', () => {
    let now = 1000;
    const s = new TranscriptStore(':memory:', { now: () => now });
    feed(s, 50);
    s.registerConsumer('fast', 'sess', { ttlMs: 10_000 });
    s.registerConsumer('slow', 'sess', { ttlMs: 10_000 });
    s.ack('fast', 'sess', 50);
    s.ack('slow', 'sess', 20);

    const stats = s.compact('sess');
    expect(stats.reclaimedRevisions).toBe(20);
    expect(stats.firstRevision).toBe(21);
    expect(stats.lastRevision).toBe(50);
    expect(stats.liveConsumers).toBe(2);
    expect(stats.checkpointRevision).toBe(50);
    expect(stats.bytesReclaimed).toBeGreaterThan(0);

    // fast consumer unaffected; slow consumer keeps reading from 21
    expect(s.poll('fast', 'sess', 100).entries).toHaveLength(0);
    const { entries } = s.poll('slow', 'sess', 100);
    expect(entries.map((e) => e.revision)).toEqual(Array.from({ length: 30 }, (_, i) => i + 21));

    const st = s.storageStats('sess');
    expect(st.storedRevisions).toBe(30);
    expect(st.checkpoints).toHaveLength(1);
    s.close();
  });

  it('compaction is a no-op when a live consumer holds the floor at the start', () => {
    const s = new TranscriptStore(':memory:');
    feed(s, 10);
    s.registerConsumer('newbie', 'sess', { ttlMs: 10_000 }); // acked 0
    const stats = s.compact('sess');
    expect(stats.reclaimedRevisions).toBe(0);
    expect(stats.firstRevision).toBe(1);
    expect(s.poll('newbie', 'sess', 100).entries).toHaveLength(10);
    s.close();
  });

  it('expired consumers stop protecting history and get reset-required on return', async () => {
    let now = 1000;
    const s = new TranscriptStore(':memory:', { now: () => now });
    feed(s, 40);
    s.registerConsumer('alive', 'sess', { ttlMs: 10_000 });
    s.registerConsumer('dying', 'sess', { ttlMs: 100 });
    s.ack('alive', 'sess', 40);
    s.ack('dying', 'sess', 5);

    now += 500; // dying's lease expired
    const stats = s.compact('sess');
    expect(stats.liveConsumers).toBe(1);
    expect(stats.firstRevision).toBe(41);

    try {
      s.poll('dying', 'sess', 100);
      expect.unreachable('poll should have thrown');
    } catch (e) {
      expect(e).toBeInstanceOf(ResetRequiredError);
      const err = e as ResetRequiredError;
      expect(err.firstAvailableRevision).toBe(41);
      expect(err.checkpointRevision).toBe(40);
    }

    // rebuild from the checkpoint archive, then resume
    let cpText = '';
    for await (const line of s.exportCheckpoint('sess')) cpText += line;
    const rebuilt = new TranscriptStore(':memory:');
    await rebuilt.importArchive(cpText);
    expect(rebuilt.snapshot('sess').text).toBe(s.snapshot('sess').text);

    const cursor = s.resetConsumerToCheckpoint('dying', 'sess');
    expect(cursor).toBe(40);
    feed(s, 10, 40);
    expect(s.poll('dying', 'sess', 100).entries.map((e) => e.revision)).toEqual(
      Array.from({ length: 10 }, (_, i) => i + 41),
    );
    s.close();
    rebuilt.close();
  });

  it('a never-registered consumer also gets reset-required, not silent skips', () => {
    const s = new TranscriptStore(':memory:');
    feed(s, 15);
    s.compact('sess'); // no live leases: reclaim everything
    expect(() => s.poll('stranger', 'sess', 100)).toThrow(ResetRequiredError);
    s.close();
  });

  it('archives exported before and after compaction both verify and rebuild the same snapshot', async () => {
    const src = new TranscriptStore(tmpDbPath('asr-comp-src-'));
    const bySource = generateSession(mulberry32(31), 2, 4);
    const model = new ReferenceModel();
    for (const [sourceId, events] of bySource) for (const e of events) model.ingest(sourceId, e);
    for (const d of makeDeliveries(mulberry32(32), bySource)) src.ingest('sess', d.sourceId, d.event);
    // a correction to make checkpoints exercise the review lineage too
    const target = [...bySource.entries()][0];
    const finalEvent = target[1].find((e) => e.kind === 'final')!;
    const base = src.lastRevision('sess');
    const lease = src.acquireLease('sess', { sourceId: target[0], eventId: finalEvent.eventId }, { actor: 'qa', baseRevision: base, ttlMs: 60_000 });
    src.submitCorrection('sess', { sourceId: target[0], eventId: finalEvent.eventId }, {
      leaseId: lease.leaseId, baseRevision: base, text: 'reviewed', actor: 'qa', reason: 'sign-off',
    });

    const preText = (await collectLines(src, 'sess')).join('\n') + '\n';
    src.registerConsumer('c', 'sess', { ttlMs: 60_000 });
    src.ack('c', 'sess', src.lastRevision('sess') - 2);
    src.compact('sess');
    const postText = (await collectLines(src, 'sess')).join('\n') + '\n';

    const wantSnap = src.snapshot('sess');
    for (const text of [preText, postText]) {
      const dst = new TranscriptStore(tmpDbPath('asr-comp-dst-'));
      const r = await dst.importArchive(text); // verifies integrity + semantics
      expect(r.status).toBe('imported');
      expect(dst.snapshot('sess')).toEqual(wantSnap);
      dst.close();
    }

    // the post-compaction archive carries the checkpoint for reset recovery
    const dst = new TranscriptStore(tmpDbPath('asr-comp-dst-'));
    await dst.importArchive(postText);
    expect(dst.latestCheckpoint('sess')).not.toBeNull();
    src.close();
    dst.close();
  });

  it('revision stream after compaction is contiguous and matches the uncompacted suffix', () => {
    const a = new TranscriptStore(':memory:');
    const b = new TranscriptStore(':memory:');
    feed(a, 60);
    feed(b, 60);
    b.registerConsumer('c', 'sess', { ttlMs: 60_000 });
    b.ack('c', 'sess', 25);
    b.compact('sess');

    const pa = a.poll('x', 'sess', 10000).entries;
    const pb = b.poll('c', 'sess', 10000).entries;
    expect(pb[0].revision).toBe(26);
    expect(pb).toEqual(pa.slice(25));
    // snapshots identical
    expect(b.snapshot('sess')).toEqual(a.snapshot('sess'));
    a.close();
    b.close();
  });

  it('concurrent compaction and writing never lose revisions or corrupt the stream', async () => {
    const file = tmpDbPath('asr-comp-conc-');
    const writer = new TranscriptStore(file);
    const compactor = new TranscriptStore(file);
    writer.registerConsumer('reader', 'sess', { ttlMs: 60_000 });

    const N = 300;
    const writing = (async () => {
      for (let i = 0; i < N; i++) {
        writer.ingest('sess', 'mic', fin(i, `seg ${i}`));
        if (i % 3 === 0) writer.ack('reader', 'sess', Math.max(0, i - 20));
        if (i % 7 === 0) await new Promise((r) => setTimeout(r, 0));
      }
    })();
    const compacting = (async () => {
      for (let i = 0; i < 12; i++) {
        compactor.compact('sess');
        await new Promise((r) => setTimeout(r, 0));
      }
    })();
    await Promise.all([writing, compacting]);

    const st = compactor.storageStats('sess');
    expect(st.lastRevision).toBe(N);
    expect(st.storedRevisions).toBe(st.lastRevision - st.firstRevision + 1);
    // stream is contiguous from firstRevision for the protected reader
    const entries: number[] = [];
    let cursor = 0;
    for (;;) {
      const { entries: batch } = compactor.poll('reader', 'sess', 50);
      if (batch.length === 0) break;
      for (const e of batch) {
        expect(e.revision).toBeGreaterThan(cursor);
        cursor = e.revision;
        entries.push(e.revision);
        compactor.ack('reader', 'sess', e.revision);
      }
    }
    expect(entries[0]).toBeGreaterThanOrEqual(st.firstRevision);
    // snapshot consistent with a fresh reference of all events
    const model = new ReferenceModel();
    for (let i = 0; i < N; i++) model.ingest('mic', fin(i, `seg ${i}`));
    const { sessionId: _a, revision: _b, ...got } = compactor.snapshot('sess');
    const { sessionId: _c, revision: _d, ...want } = model.snapshot('sess');
    expect(got).toEqual(want);

    // archive exported from the compacted store still verifies
    const text = (await collectLines(compactor, 'sess')).join('\n') + '\n';
    const dst = new TranscriptStore(tmpDbPath('asr-comp-dst-'));
    await dst.importArchive(text);
    expect(dst.snapshot('sess').text).toBe(compactor.snapshot('sess').text);
    writer.close();
    compactor.close();
    dst.close();
  });

  it('two compactions chain checkpoints; reset uses the latest', () => {
    const s = new TranscriptStore(':memory:');
    feed(s, 30);
    s.registerConsumer('c', 'sess', { ttlMs: 60_000 });
    s.ack('c', 'sess', 10);
    const c1 = s.compact('sess');
    feed(s, 30, 30);
    s.ack('c', 'sess', 40);
    const c2 = s.compact('sess');

    expect(c1.firstRevision).toBe(11);
    expect(c2.firstRevision).toBe(41);
    expect(s.latestCheckpoint('sess')!.revision).toBe(60);
    expect(s.storageStats('sess').checkpoints).toHaveLength(2);
    expect(s.resetConsumerToCheckpoint('late', 'sess')).toBe(60);
    s.close();
  });
});
