import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { readFileSync, writeFileSync } from 'node:fs';
import {
  createTempStore,
  makeEvent,
} from '../helpers';
import { forceRemove } from '../cleanup';
import {
  RecognitionStore,
  ArchiveChecksumError,
  ArchiveFormatError,
  ArchiveVersionError,
  SessionExistsError,
} from '../../src';
import { parseArchiveBuffer } from '../../src/archive';

describe('session archive', () => {
  let store: RecognitionStore;
  let cleanup: () => void;
  let tmpDir: string;

  beforeEach(() => {
    const ctx = createTempStore();
    store = ctx.store;
    cleanup = ctx.cleanup;
    tmpDir = mkdtempSync(join(tmpdir(), 'asr-archive-'));
  });
  afterEach(() => {
    cleanup();
    forceRemove(tmpDir);
  });

  function buildRichSession(sessionId = 'archive-session') {
    const session = store.session(sessionId);
    session.ingest(makeEvent('srcA', 1, 'final', 'hello ', 'e1'));
    session.ingest(makeEvent('srcA', 2, 'final', 'world', 'e2'));
    session.ingest(makeEvent('srcB', 1, 'partial', 'foo', 'e3'));

    const lease = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'reviewer-1',
      baseRevision: 2,
    });
    session.submitCorrection({
      leaseId: lease.leaseId,
      correctedContent: 'Hello ',
      reason: 'capitalization',
    });

    const consumer = store.consumer(sessionId, 'qc-1');
    const batch = consumer.read(10);
    consumer.ack(batch[batch.length - 1].revision);

    return { session, consumer };
  }

  it('exports and re-imports preserving snapshot, revisions, and corrections', async () => {
    const { session: src } = buildRichSession('rt-session');
    const archivePath = join(tmpDir, 'rt.asra');
    const exportResult = await store.exportSession('rt-session', archivePath);

    expect(exportResult.counts.revisions).toBeGreaterThan(0);
    expect(exportResult.counts.corrections).toBe(1);
    expect(exportResult.counts.cursors).toBe(1);

    const importStore = RecognitionStore.open({
      dbPath: join(tmpDir, 'imported.db'),
    });
    try {
      const result = importStore.importSession(archivePath);
      expect(result.idempotent).toBe(false);
      expect(result.sessionId).toBe('rt-session');

      const imported = importStore.session('rt-session');
      const srcSnap = src.getSnapshot();
      const dstSnap = imported.getSnapshot();

      expect(dstSnap.text).toBe(srcSnap.text);
      expect(dstSnap.revision).toBe(srcSnap.revision);
      expect(dstSnap.summary).toEqual(srcSnap.summary);

      expect(dstSnap.text).toBe('Hello worldfoo');

      const srcRevs = src.getRevisionsSince(0, 1000);
      const dstRevs = imported.getRevisionsSince(0, 1000);
      expect(dstRevs).toHaveLength(srcRevs.length);
      for (let i = 0; i < srcRevs.length; i++) {
        expect(dstRevs[i].revision).toBe(srcRevs[i].revision);
        expect(dstRevs[i].changeType).toBe(srcRevs[i].changeType);
        expect(dstRevs[i].snapshotText).toBe(srcRevs[i].snapshotText);
        expect(dstRevs[i].correctionId).toBe(srcRevs[i].correctionId);
      }

      const corrections = imported.getCorrectionsForFragment('srcA', 1);
      expect(corrections).toHaveLength(1);
      expect(corrections[0].actor).toBe('reviewer-1');
      expect(corrections[0].reason).toBe('capitalization');
      expect(corrections[0].originalContent).toBe('hello ');
      expect(corrections[0].correctedContent).toBe('Hello ');
      expect(corrections[0].supersedesCorrectionId).toBeNull();

      const importedConsumer = importStore.consumer('rt-session', 'qc-1');
      expect(importedConsumer.getCursor()).toBe(src.getRevisionsSince(0, 1000).length);
    } finally {
      importStore.close();
    }
  });

  it('re-importing the same archive is idempotent', async () => {
    buildRichSession('idem-session');
    const archivePath = join(tmpDir, 'idem.asra');
    await store.exportSession('idem-session', archivePath);

    const importStore = RecognitionStore.open({
      dbPath: join(tmpDir, 'idem.db'),
    });
    try {
      const r1 = importStore.importSession(archivePath);
      expect(r1.idempotent).toBe(false);

      const r2 = importStore.importSession(archivePath);
      expect(r2.idempotent).toBe(true);

      const r3 = importStore.importSession(archivePath);
      expect(r3.idempotent).toBe(true);
    } finally {
      importStore.close();
    }
  });

  it('rejects importing into a session with different content', async () => {
    buildRichSession('conflict-src');
    const archivePath = join(tmpDir, 'conflict.asra');
    await store.exportSession('conflict-src', archivePath);

    const importStore = RecognitionStore.open({
      dbPath: join(tmpDir, 'conflict.db'),
    });
    try {
      const s = importStore.session('conflict-src');
      s.ingest(makeEvent('srcZ', 99, 'final', 'different', 'dx'));

      expect(() => importStore.importSession(archivePath)).toThrow(
        SessionExistsError,
      );
    } finally {
      importStore.close();
    }
  });

  it('detects a tampered payload via checksum', async () => {
    buildRichSession('tamper-session');
    const archivePath = join(tmpDir, 'tamper.asra');
    await store.exportSession('tamper-session', archivePath);

    const buf = readFileSync(archivePath);
    const tampered = Buffer.from(buf);
    const contentIdx = tampered.indexOf(Buffer.from('Hello '));
    expect(contentIdx).toBeGreaterThan(-1);
    tampered[contentIdx] = tampered[contentIdx] ^ 0xff;
    writeFileSync(archivePath, tampered);

    expect(() => parseArchiveBuffer(tampered)).toThrow(ArchiveChecksumError);

    const importStore = RecognitionStore.open({
      dbPath: join(tmpDir, 'tamper-import.db'),
    });
    try {
      expect(() => importStore.importSession(archivePath)).toThrow(
        ArchiveChecksumError,
      );
      const row = importStore
        .session('tamper-session')
        .getLatestRevisionNumber();
      expect(row).toBe(0);
    } finally {
      importStore.close();
    }
  });

  it('detects a truncated archive', async () => {
    buildRichSession('trunc-session');
    const archivePath = join(tmpDir, 'trunc.asra');
    await store.exportSession('trunc-session', archivePath);

    const buf = readFileSync(archivePath);
    const truncated = buf.subarray(0, buf.length - 30);
    writeFileSync(archivePath, truncated);

    expect(() => parseArchiveBuffer(truncated)).toThrow();

    const importStore = RecognitionStore.open({
      dbPath: join(tmpDir, 'trunc-import.db'),
    });
    try {
      expect(() => importStore.importSession(archivePath)).toThrow();
      expect(
        importStore.session('trunc-session').getLatestRevisionNumber(),
      ).toBe(0);
    } finally {
      importStore.close();
    }
  });

  it('rejects invalid magic bytes', () => {
    const bad = Buffer.from('XXXX' + '\x00\x00\x00\x01');
    expect(() => parseArchiveBuffer(bad)).toThrow(ArchiveFormatError);
  });

  it('rejects a newer format version', () => {
    const buf = Buffer.alloc(8);
    buf.write('ASRA', 0, 'ascii');
    buf.writeUInt32LE(99, 4);
    expect(() => parseArchiveBuffer(buf)).toThrow(ArchiveVersionError);
  });

  it('preserves correction lineage with supersedes chain', async () => {
    const session = store.session('lineage-session');
    session.ingest(makeEvent('srcA', 1, 'final', 'v0', 'e1'));

    const l1 = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'alice',
      baseRevision: 1,
    });
    const c1 = session.submitCorrection({
      leaseId: l1.leaseId,
      correctedContent: 'v1',
      reason: 'first',
    });

    const l2 = session.acquireLease({
      sourceId: 'srcA',
      sourceSeq: 1,
      actor: 'bob',
      baseRevision: c1.revision,
    });
    const c2 = session.submitCorrection({
      leaseId: l2.leaseId,
      correctedContent: 'v2',
      reason: 'second',
    });

    const archivePath = join(tmpDir, 'lineage.asra');
    await store.exportSession('lineage-session', archivePath);

    const importStore = RecognitionStore.open({
      dbPath: join(tmpDir, 'lineage.db'),
    });
    try {
      importStore.importSession(archivePath);
      const imported = importStore.session('lineage-session');
      const corrections = imported.getCorrectionsForFragment('srcA', 1);

      expect(corrections).toHaveLength(2);
      expect(corrections[0].correctionId).toBe(c1.correctionId);
      expect(corrections[0].supersedesCorrectionId).toBeNull();
      expect(corrections[1].correctionId).toBe(c2.correctionId);
      expect(corrections[1].supersedesCorrectionId).toBe(c1.correctionId);
      expect(imported.getSnapshot().text).toBe('v2');
    } finally {
      importStore.close();
    }
  });

  it('preserves multiple independent consumer cursors', async () => {
    const session = store.session('multi-cursor');
    for (let i = 0; i < 5; i++) {
      session.ingest(makeEvent('srcA', i, 'final', `t${i}`, `e${i}`));
    }

    const c1 = store.consumer('multi-cursor', 'fast');
    const c2 = store.consumer('multi-cursor', 'slow');
    c1.ack(5);
    c2.ack(2);

    const archivePath = join(tmpDir, 'cursors.asra');
    await store.exportSession('multi-cursor', archivePath);

    const importStore = RecognitionStore.open({
      dbPath: join(tmpDir, 'cursors.db'),
    });
    try {
      importStore.importSession(archivePath);
      const i1 = importStore.consumer('multi-cursor', 'fast');
      const i2 = importStore.consumer('multi-cursor', 'slow');

      expect(i1.getCursor()).toBe(5);
      expect(i2.getCursor()).toBe(2);
      expect(i1.read(100)).toEqual([]);
      expect(i2.read(100)).toHaveLength(3);
    } finally {
      importStore.close();
    }
  });

  it('rejects archive with missing checksum trailer', () => {
    const buf = Buffer.from('ASRA');
    const ver = Buffer.alloc(4);
    ver.writeUInt32LE(1, 0);
    const noTrailer = Buffer.concat([buf, ver]);
    expect(() => parseArchiveBuffer(noTrailer)).toThrow(ArchiveFormatError);
  });

  it('rejects archive with count mismatch', () => {
    const magic = Buffer.from('ASRA');
    const ver = Buffer.alloc(4);
    ver.writeUInt32LE(1, 0);
    const header = encodeSeg('H', JSON.stringify({
      sessionId: 'x',
      exportedAt: Date.now(),
      formatVersion: 1,
      counts: {
        session: 1, events: 99, fragments: 0, revisions: 0,
        corrections: 0, leases: 0, cursors: 0,
      },
    }));
    const sessionSeg = encodeSeg('S', JSON.stringify({
      session_id: 'x', created_at: 1, updated_at: 1, latest_revision: 0,
    }));
    const checksum = '0'.repeat(64);
    const trailer = encodeSeg('X', JSON.stringify({
      sha256: checksum,
      counts: {
        session: 1, events: 99, fragments: 0, revisions: 0,
        corrections: 0, leases: 0, cursors: 0,
      },
    }));
    const data = Buffer.concat([magic, ver, header, sessionSeg]);
    const { createHash } = require('node:crypto');
    const h = createHash('sha256').update(data).digest('hex');
    const realTrailer = encodeSeg('X', JSON.stringify({
      sha256: h,
      counts: {
        session: 1, events: 99, fragments: 0, revisions: 0,
        corrections: 0, leases: 0, cursors: 0,
      },
    }));
    const archive = Buffer.concat([data, realTrailer]);
    expect(() => parseArchiveBuffer(archive)).toThrow(ArchiveFormatError);
  });
});

function encodeSeg(type: string, json: string): Buffer {
  const payload = Buffer.from(json, 'utf8');
  const buf = Buffer.alloc(5 + payload.length);
  buf.write(type, 0, 'ascii');
  buf.writeUInt32LE(payload.length, 1);
  payload.copy(buf, 5);
  return buf;
}
