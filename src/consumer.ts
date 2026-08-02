import type { Database as DatabaseType } from 'better-sqlite3';
import type { RevisionData } from './types';
import { Session } from './session';

export interface StreamOptions {
  batchSize?: number;
  pollIntervalMs?: number;
  signal?: AbortSignal;
}

export class Consumer {
  private readonly getCursorStmt;
  private readonly upsertCursorStmt;
  private readonly ackTxn: ReturnType<DatabaseType['transaction']>;

  constructor(
    db: DatabaseType,
    private readonly session: Session,
    public readonly consumerId: string,
  ) {
    this.session.ensureExists();

    this.getCursorStmt = db.prepare(
      `SELECT cursor_revision FROM consumer_cursors
       WHERE session_id = ? AND consumer_id = ?`,
    );
    this.upsertCursorStmt = db.prepare(
      `INSERT INTO consumer_cursors
         (session_id, consumer_id, cursor_revision, updated_at)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(session_id, consumer_id) DO UPDATE SET
         cursor_revision = MAX(consumer_cursors.cursor_revision, excluded.cursor_revision),
         updated_at     = excluded.updated_at`,
    );

    this.ackTxn = db.transaction((revision: number) => {
      const now = Date.now();
      this.upsertCursorStmt.run(
        this.session.sessionId,
        this.consumerId,
        revision,
        now,
      );
    });
  }

  getCursor(): number {
    const row = this.getCursorStmt.get(
      this.session.sessionId,
      this.consumerId,
    ) as { cursor_revision: number } | undefined;
    return row ? row.cursor_revision : 0;
  }

  read(limit = 100): RevisionData[] {
    const cursor = this.getCursor();
    return this.session.getRevisionsSince(cursor, limit);
  }

  ack(revision: number): void {
    this.ackTxn.immediate(revision);
  }

  async *stream(options: StreamOptions = {}): AsyncIterable<RevisionData[]> {
    const batchSize = options.batchSize ?? 100;
    const pollInterval = options.pollIntervalMs ?? 200;
    const signal = options.signal;

    while (true) {
      if (signal?.aborted) {
        return;
      }

      const batch = this.read(batchSize);
      if (batch.length > 0) {
        yield batch;
      } else {
        await sleep(pollInterval, signal);
      }
    }
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise<void>((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      resolve();
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}
