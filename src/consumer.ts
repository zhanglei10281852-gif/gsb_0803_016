import type { Database as DatabaseType } from 'better-sqlite3';
import type {
  RevisionData,
  ConsumerLeaseInfo,
  ConsumerOptions,
} from './types';
import { Session } from './session';
import { ConsumerResetRequiredError } from './errors';

const DEFAULT_LEASE_TTL_MS = 24 * 60 * 60 * 1000;

export interface StreamOptions {
  batchSize?: number;
  pollIntervalMs?: number;
  signal?: AbortSignal;
}

interface CursorRow {
  cursor_revision: number;
  lease_expires_at: number | null;
}

export class Consumer {
  private readonly getCursorStmt;
  private readonly upsertCursorStmt;
  private readonly touchLeaseStmt;
  private readonly clearLeaseStmt;
  private readonly getCompactionWatermarkStmt;
  private readonly ackTxn: ReturnType<DatabaseType['transaction']>;
  private readonly heartbeatTxn: ReturnType<DatabaseType['transaction']>;
  private readonly leaseTtlMs: number;

  constructor(
    db: DatabaseType,
    private readonly session: Session,
    public readonly consumerId: string,
    options?: ConsumerOptions,
  ) {
    this.session.ensureExists();
    this.leaseTtlMs = options?.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS;

    this.getCursorStmt = db.prepare(
      `SELECT cursor_revision, lease_expires_at
       FROM consumer_cursors
       WHERE session_id = ? AND consumer_id = ?`,
    );
    this.upsertCursorStmt = db.prepare(
      `INSERT INTO consumer_cursors
         (session_id, consumer_id, cursor_revision, updated_at, lease_expires_at)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(session_id, consumer_id) DO UPDATE SET
         cursor_revision  = MAX(consumer_cursors.cursor_revision, excluded.cursor_revision),
         updated_at       = excluded.updated_at,
         lease_expires_at = excluded.lease_expires_at`,
    );
    this.touchLeaseStmt = db.prepare(
      `UPDATE consumer_cursors
       SET lease_expires_at = ?, updated_at = ?
       WHERE session_id = ? AND consumer_id = ?`,
    );
    this.clearLeaseStmt = db.prepare(
      `UPDATE consumer_cursors
       SET lease_expires_at = NULL
       WHERE session_id = ? AND consumer_id = ?`,
    );
    this.getCompactionWatermarkStmt = db.prepare(
      `SELECT COALESCE(
         (SELECT MIN(revision) - 1 FROM revisions WHERE session_id = ?),
         (SELECT COALESCE(MAX(max_revision), 0) FROM compaction_checkpoints WHERE session_id = ?)
       ) AS watermark`,
    );

    this.ackTxn = db.transaction((revision: number) => {
      const now = Date.now();
      const expires = now + this.leaseTtlMs;
      this.upsertCursorStmt.run(
        this.session.sessionId,
        this.consumerId,
        revision,
        now,
        expires,
      );
    });

    this.heartbeatTxn = db.transaction(() => {
      const now = Date.now();
      const expires = now + this.leaseTtlMs;
      this.touchLeaseStmt.run(
        expires,
        now,
        this.session.sessionId,
        this.consumerId,
      );
    });
  }

  getCursor(): number {
    const row = this.getCursorStmt.get(
      this.session.sessionId,
      this.consumerId,
    ) as CursorRow | undefined;
    return row ? row.cursor_revision : 0;
  }

  getLeaseInfo(): ConsumerLeaseInfo {
    const row = this.getCursorStmt.get(
      this.session.sessionId,
      this.consumerId,
    ) as CursorRow | undefined;
    const now = Date.now();
    return {
      consumerId: this.consumerId,
      cursorRevision: row ? row.cursor_revision : 0,
      leaseExpiresAt: row?.lease_expires_at ?? null,
      active: !!row?.lease_expires_at && row.lease_expires_at > now,
    };
  }

  heartbeat(): void {
    this.heartbeatTxn.immediate();
  }

  releaseLease(): void {
    this.clearLeaseStmt.run(this.session.sessionId, this.consumerId);
  }

  read(limit = 100): RevisionData[] {
    const cursor = this.getCursor();
    const watermark = this.getCompactionWatermark();
    const currentRevision = this.session.getLatestRevisionNumber();

    if (cursor < watermark) {
      throw new ConsumerResetRequiredError(
        `consumer '${this.consumerId}' cursor ${cursor} is below compaction watermark ${watermark}; reset required`,
        watermark,
        currentRevision,
        watermark,
      );
    }

    return this.session.getRevisionsSince(cursor, limit);
  }

  ack(revision: number): void {
    this.ackTxn.immediate(revision);
  }

  getSnapshot() {
    return this.session.getSnapshot();
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

  private getCompactionWatermark(): number {
    const row = this.getCompactionWatermarkStmt.get(
      this.session.sessionId,
      this.session.sessionId,
    ) as { watermark: number } | undefined;
    return row ? row.watermark : 0;
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
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
