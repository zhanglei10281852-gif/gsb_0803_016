import assert from 'node:assert/strict';
import { ReferenceModel, ResetRequiredError, TranscriptStore } from '../dist/index.js';
import { sleep, tmpDbPath } from './lib.mjs';

/**
 * Long-session compaction: a writer streams events while a second instance
 * compacts periodically. A live (leased) consumer must never see gaps; a
 * consumer whose lease lapses must get an explicit reset-required and
 * recover from the checkpoint. Compaction stats stay observable throughout,
 * and the compacted store still exports verifiable, equivalent archives.
 */
export async function main() {
  const file = tmpDbPath('asr-e2e-comp-');
  const writer = new TranscriptStore(file);
  const compactor = new TranscriptStore(file);
  const N = 400;

  writer.registerConsumer('live', 'sess', { ttlMs: 30_000 });
  writer.registerConsumer('flaky', 'sess', { ttlMs: 120 });

  const model = new ReferenceModel();
  let liveCursor = 0;
  let compactions = 0;
  let reclaimed = 0;

  const writing = (async () => {
    for (let i = 0; i < N; i++) {
      const ev = {
        eventId: `e${i}`,
        sourceSeq: i,
        kind: i % 4 === 3 ? 'final' : 'partial',
        text: `segment ${i}`,
        startMs: i * 40,
      };
      writer.ingest('sess', 'mic', ev);
      model.ingest('mic', ev);
      // live consumer trails ~30 revisions behind and keeps its lease fresh
      if (i % 5 === 0) {
        writer.registerConsumer('live', 'sess', { ttlMs: 30_000 });
        const { entries } = writer.poll('live', 'sess', 20);
        for (const e of entries) liveCursor = writer.ack('live', 'sess', e.revision);
      }
      if (i % 11 === 0) await sleep(0);
    }
  })();

  const compacting = (async () => {
    for (let i = 0; i < 10; i++) {
      await sleep(2);
      const stats = compactor.compact('sess');
      compactions++;
      reclaimed += stats.reclaimedRevisions;
      assert.ok(stats.bytesReclaimed >= 0 && stats.storedRevisions !== -1);
    }
  })();
  await Promise.all([writing, compacting]);

  const st = compactor.storageStats('sess');
  assert.equal(st.lastRevision, N);
  assert.equal(st.storedRevisions, st.lastRevision - st.firstRevision + 1);
  assert.ok(compactions > 0 && reclaimed > 0, 'compaction actually reclaimed');
  assert.ok(st.checkpoints.length > 0, 'checkpoints recorded');
  assert.ok(st.consumers.find((c) => c.consumerId === 'live').leaseLive);
  assert.ok(!st.consumers.find((c) => c.consumerId === 'flaky').leaseLive);

  // live consumer: never reset, contiguous from wherever compaction floor was
  const { entries } = compactor.poll('live', 'sess', 10000);
  assert.deepEqual(
    entries.map((e) => e.revision),
    Array.from({ length: entries.length }, (_, i) => i + liveCursor + 1),
  );

  // flaky consumer: explicit reset-required, then recover from checkpoint
  assert.throws(() => compactor.poll('flaky', 'sess', 10), ResetRequiredError);
  let cpText = '';
  for await (const line of compactor.exportCheckpoint('sess')) cpText += line;
  const rebuilt = new TranscriptStore(tmpDbPath('asr-e2e-comp-rb-'));
  const ri = await rebuilt.importArchive(cpText);
  assert.equal(ri.status, 'imported');
  // the checkpoint is a past consistent state: its text prefixes the current one
  const currentText = compactor.snapshot('sess').text;
  assert.ok(currentText.startsWith(rebuilt.snapshot('sess').text));
  assert.ok(ri.lastRevision < st.lastRevision, 'checkpoint predates the latest writes');
  const resumed = compactor.resetConsumerToCheckpoint('flaky', 'sess');
  assert.equal(resumed, st.checkpoints.at(-1).revision);
  writer.ingest('sess', 'mic', { eventId: 'tail', sourceSeq: N, kind: 'final', text: 'tail', startMs: N * 40 });
  assert.equal(compactor.poll('flaky', 'sess', 10000).entries.at(-1).change.text, 'tail');

  // snapshot vs reference model; archive still verifies post-compaction
  const { sessionId: _a, revision: _b, ...got } = compactor.snapshot('sess');
  model.ingest('mic', { eventId: 'tail', sourceSeq: N, kind: 'final', text: 'tail', startMs: N * 40 });
  const { sessionId: _c, revision: _d, ...want } = model.snapshot('sess');
  assert.deepEqual(got, want);

  let archiveText = '';
  for await (const line of compactor.exportArchive('sess')) archiveText += line;
  const dst = new TranscriptStore(tmpDbPath('asr-e2e-comp-dst-'));
  assert.equal((await dst.importArchive(archiveText)).status, 'imported');
  assert.equal(dst.snapshot('sess').text, compactor.snapshot('sess').text);
  assert.ok(dst.latestCheckpoint('sess'), 'checkpoint carried by archive');

  writer.close();
  compactor.close();
  rebuilt.close();
  dst.close();
  console.log(
    `e2e/compaction: OK (${reclaimed} revisions reclaimed over ${compactions} compactions, floor=${st.firstRevision})`,
  );
}

if (process.argv[1] && process.argv[1].endsWith('compaction.mjs')) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
