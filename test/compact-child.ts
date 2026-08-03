import { createRecognitionStore } from "../src";
import type { RecognitionEventInput } from "../src";

interface CompactChildRequest {
  filename: string;
  sessionId: string;
  rounds: number;
  writer?: {
    sourceId: string;
    count: number;
    delayMs: number;
    seed: number;
  };
}

function wait(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

process.on("message", async (message: CompactChildRequest) => {
  const store = createRecognitionStore({
    filename: message.filename,
    busyTimeout: 30000,
  });

  try {
    if (message.writer) {
      const { sourceId, count, delayMs, seed } = message.writer;
      for (let i = 0; i < count; i += 1) {
        const event: RecognitionEventInput = {
          eventId: `${sourceId}-final-${i}`,
          sourceId,
          sourceSeq: i,
          isFinal: true,
          text: `${sourceId}:${i} `,
        };
        store.ingest(message.sessionId, event);
        const jitter = ((seed + i * 7) % delayMs) + 1;
        wait(jitter);
      }
    } else {
      let compactions = 0;
      for (let round = 0; round < message.rounds; round += 1) {
        const snapshot = store.getSnapshot(message.sessionId);
        if (snapshot.revision === 0) {
          wait(5);
          continue;
        }

        let archive = "";
        for await (const chunk of store.exportArchive(message.sessionId)) {
          archive += chunk;
        }
        const header = JSON.parse(archive.split("\n")[0] as string) as {
          dataHash: string;
          recordCount: number;
        };
        store.registerCheckpoint(message.sessionId, {
          revision: snapshot.revision,
          archiveHash: header.dataHash,
          recordCount: header.recordCount,
        });

        const consumer = store.createConsumer(
          message.sessionId,
          "compactor-lease",
        );
        consumer.heartbeat(60000);
        consumer.acknowledge(snapshot.revision);

        const result = store.compact(message.sessionId);
        if (result.compacted) compactions += 1;
        wait(3);
      }
      if (compactions === 0) {
        throw new Error("compactor did not complete any compaction");
      }
    }

    process.send?.({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.stack ?? error.message : String(error);
    process.stderr.write(`compact-child error: ${message}\n`);
    process.send?.({
      ok: false,
      message,
    });
    process.exitCode = 1;
  } finally {
    store.close();
  }
});
