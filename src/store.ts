import type { Database as DatabaseType } from 'better-sqlite3';
import { openDatabase } from './db';
import { Session } from './session';
import { Consumer } from './consumer';
import { StoreClosedError } from './errors';
import type { StoreOptions } from './types';

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

  consumer(sessionId: string, consumerId: string): Consumer {
    if (!this.db) throw new StoreClosedError();
    const session = this.session(sessionId);
    return new Consumer(this.db, session, consumerId);
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
