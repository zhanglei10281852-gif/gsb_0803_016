const path = require('path');
const Database = require('better-sqlite3');
const { RecognitionStore } = require(path.resolve(__dirname, '..', '..', 'dist', 'index.js'));

function run() {
  let raw = '';
  process.stdin.on('data', (chunk) => { raw += chunk; });
  process.stdin.on('end', () => {
    const cmd = JSON.parse(raw);

    if (cmd.action === 'crash-mid-txn') {
      const initStore = RecognitionStore.open({ dbPath: cmd.dbPath, busyTimeoutMs: 30000 });
      initStore.session(cmd.sessionId).ensureExists();
      initStore.close();

      const db = new Database(cmd.dbPath);
      db.pragma('journal_mode = WAL');
      db.pragma('synchronous = FULL');
      db.pragma('busy_timeout = 30000');
      db.exec('BEGIN IMMEDIATE');
      const now = Date.now();
      for (const e of cmd.events) {
        db.prepare(`INSERT INTO incoming_events (session_id, event_id, source_id, source_seq, event_type, content, content_hash, resulted_revision, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)`).run(
          cmd.sessionId, e.eventId, e.sourceId, e.sourceSeq, e.type, e.content, 'crash-hash', now
        );
      }
      process.kill(process.pid, 'SIGKILL');
      return;
    }

    if (cmd.action === 'ingest-then-crash') {
      const store = RecognitionStore.open({ dbPath: cmd.dbPath, busyTimeoutMs: 30000 });
      const session = store.session(cmd.sessionId);
      for (let i = 0; i < cmd.events.length; i++) {
        session.ingest(cmd.events[i]);
      }
      process.kill(process.pid, 'SIGKILL');
      return;
    }

    const store = RecognitionStore.open({ dbPath: cmd.dbPath, busyTimeoutMs: 30000 });
    try {
      if (cmd.action === 'ingest') {
        const session = store.session(cmd.sessionId);
        const results = [];
        for (const e of cmd.events) {
          results.push(session.ingest(e));
        }
        const snap = session.getSnapshot();
        process.stdout.write(JSON.stringify({
          ok: true,
          results,
          snapshot: { text: snap.text, revision: snap.revision, summary: snap.summary },
        }));
      } else if (cmd.action === 'snapshot') {
        const session = store.session(cmd.sessionId);
        const snap = session.getSnapshot();
        process.stdout.write(JSON.stringify({
          ok: true,
          snapshot: { text: snap.text, revision: snap.revision, summary: snap.summary },
        }));
      } else if (cmd.action === 'consumer-read') {
        const consumer = store.consumer(cmd.sessionId, cmd.consumerId);
        const batch = consumer.read(cmd.limit || 100);
        process.stdout.write(JSON.stringify({
          ok: true,
          cursor: consumer.getCursor(),
          revisions: batch.map(r => ({ revision: r.revision, sourceId: r.sourceId, sourceSeq: r.sourceSeq, changeType: r.changeType })),
        }));
      } else if (cmd.action === 'consumer-ack') {
        const consumer = store.consumer(cmd.sessionId, cmd.consumerId);
        consumer.ack(cmd.revision);
        process.stdout.write(JSON.stringify({ ok: true, cursor: consumer.getCursor() }));
      } else if (cmd.action === 'revisions-count') {
        const session = store.session(cmd.sessionId);
        const db = new Database(cmd.dbPath, { readonly: true });
        const row = db.prepare('SELECT COUNT(*) AS c FROM revisions WHERE session_id = ?').get(cmd.sessionId);
        db.close();
        process.stdout.write(JSON.stringify({ ok: true, count: row.c, latest: session.getLatestRevisionNumber() }));
      } else {
        process.stdout.write(JSON.stringify({ ok: false, error: 'unknown action: ' + cmd.action }));
      }
    } catch (err) {
      process.stdout.write(JSON.stringify({ ok: false, error: err.message, code: err.code }));
    } finally {
      try { store.close(); } catch {}
    }
  });
}

run();
