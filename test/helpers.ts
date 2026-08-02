import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { RecognitionStore } from '../src';
import { forceRemove } from './cleanup';

export function createTempStore(prefix = 'asr-conf-'): {
  store: RecognitionStore;
  dir: string;
  dbPath: string;
  cleanup: () => void;
} {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  const dbPath = join(dir, 'test.db');
  const store = RecognitionStore.open({ dbPath, busyTimeoutMs: 5000 });
  const cleanup = () => {
    try {
      store.close();
    } catch {
      // ignore
    }
    forceRemove(dir);
  };
  return { store, dir, dbPath, cleanup };
}

export function makeEvent(
  sourceId: string,
  sourceSeq: number,
  type: 'partial' | 'final',
  content: string,
  eventId?: string,
) {
  return {
    eventId: eventId ?? `${sourceId}-${sourceSeq}-${type}`,
    sourceId,
    sourceSeq,
    type,
    content,
  };
}

export function shuffle<T>(arr: T[]): T[] {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}
