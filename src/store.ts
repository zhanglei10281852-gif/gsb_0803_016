import type { Database as DatabaseType } from 'better-sqlite3';
import { openDatabase } from './db';
import { Session } from './session';
import { Consumer } from './consumer';
import { StoreClosedError } from './errors';
import {
  writeSessionArchive,
  importSessionArchive,
  type ExportResult,
  type ImportResult,
} from './archive';
import { compactSession, recordCheckpoint } from './compaction';
import type { StoreOptions, CompactionStats, ConsumerOptions } from './types';

export class RecognitionStore {
  private db: DatabaseType | null = null;
  private readonly closeDb: () => void;
  private readonly sessions = new Map<string, Session>();

  private constructor(db: DatabaseType, closeDb: () => void) {
    this.db = db;
    this.closeDb = closeDb;
  }

  static open(options: StoreOptions): RecognitionStore {
    const { db, close } = openDatabase(
      options.dbPath,
      options.busyTimeoutMs ?? 10000,
    );
    return new RecognitionStore(db, close);
  }

  session(sessionId: string): Session {
    if (!this.db) throw new StoreClosedError();
    let s = this.sessions.get(sessionId);
    if (!s) {
      s = new Session(this.db, sessionId);
      this.sessions.set(sessionId, s);
    }
    return s;
  }

  consumer(
    sessionId: string,
    consumerId: string,
    options?: ConsumerOptions,
  ): Consumer {
    if (!this.db) throw new StoreClosedError();
    const session = this.session(sessionId);
    return new Consumer(this.db, session, consumerId, options);
  }

  async exportSession(
    sessionId: string,
    filePath: string,
  ): Promise<ExportResult> {
    if (!this.db) throw new StoreClosedError();
    const result = await writeSessionArchive(this.db, sessionId, filePath);

    recordCheckpoint(
      this.db,
      sessionId,
      filePath,
      result.sha256,
      1,
      result.maxRevision,
      result.counts.revisions,
      result.counts.events,
    );

    return result;
  }

  importSession(archivePath: string): ImportResult {
    if (!this.db) throw new StoreClosedError();
    return importSessionArchive(this.db, archivePath);
  }

  compactSession(sessionId: string): CompactionStats {
    if (!this.db) throw new StoreClosedError();
    return compactSession(this.db, sessionId);
  }

  close(): void {
    if (!this.db) return;
    this.sessions.clear();
    this.closeDb();
    this.db = null;
  }

  get closed(): boolean {
    return this.db === null;
  }
}
