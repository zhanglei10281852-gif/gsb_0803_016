import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTempStore, makeEvent } from '../helpers';
import { EventConflictError, RecognitionStore } from '../../src';

describe('idempotency', () => {
  let store: RecognitionStore;
  let cleanup: () => void;

  beforeEach(() => {
    const ctx = createTempStore();
    store = ctx.store;
    cleanup = ctx.cleanup;
  });
  afterEach(() => cleanup());

  it('returns duplicate for identical eventId with same content', () => {
    const session = store.session('s1');
    const event = makeEvent('srcA', 1, 'partial', 'hello');
    const r1 = session.ingest(event);
    expect(r1.status).toBe('accepted');
    expect(r1.revision).toBe(1);

    const r2 = session.ingest({ ...event });
    expect(r2.status).toBe('duplicate');
    expect(r2.revision).toBe(1);
  });

  it('rejects same eventId with different content', () => {
    const session = store.session('s2');
    session.ingest(makeEvent('srcA', 1, 'partial', 'hello', 'e1'));
    expect(() =>
      session.ingest(makeEvent('srcA', 1, 'partial', 'world', 'e1')),
    ).toThrow(EventConflictError);
  });

  it('rejects same eventId with different type', () => {
    const session = store.session('s3');
    session.ingest(makeEvent('srcA', 1, 'partial', 'hello', 'e2'));
    expect(() =>
      session.ingest(makeEvent('srcA', 1, 'final', 'hello', 'e2')),
    ).toThrow(EventConflictError);
  });

  it('duplicate of ignored event returns duplicate with null revision', () => {
    const session = store.session('s4');
    session.ingest(makeEvent('srcA', 1, 'final', 'done', 'f1'));
    const ignored = session.ingest(
      makeEvent('srcA', 1, 'partial', 'late', 'p1'),
    );
    expect(ignored.status).toBe('ignored');
    expect(ignored.revision).toBeNull();

    const dup = session.ingest(makeEvent('srcA', 1, 'partial', 'late', 'p1'));
    expect(dup.status).toBe('duplicate');
    expect(dup.revision).toBeNull();
  });

  it('does not create extra revisions on duplicate', () => {
    const session = store.session('s5');
    session.ingest(makeEvent('srcA', 1, 'partial', 'a', 'e1'));
    session.ingest(makeEvent('srcA', 1, 'partial', 'a', 'e1'));
    session.ingest(makeEvent('srcA', 1, 'partial', 'a', 'e1'));
    expect(session.getLatestRevisionNumber()).toBe(1);
  });
});
