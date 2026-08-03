import { createRecognitionStore } from "../src";
import type { RecognitionEventInput } from "../src";

interface ChildRequest {
  filename: string;
  events: RecognitionEventInput[];
  seed: number;
}

function randomDelay(seed: number): number {
  let value = (seed * 1664525 + 1013904223) >>> 0;
  return 1 + (value % 5);
}

process.on("message", (message: ChildRequest) => {
  const store = createRecognitionStore({
    filename: message.filename,
    busyTimeout: 30000,
  });
  const waitBuffer = new Int32Array(new SharedArrayBuffer(4));

  try {
    message.events.forEach((event, index) => {
      store.ingest("session", event);
      Atomics.wait(
        waitBuffer,
        0,
        0,
        randomDelay(message.seed + index)
      );
    });
    process.send?.({ ok: true });
  } catch (error) {
    process.send?.({
      ok: false,
      message: error instanceof Error ? error.stack ?? error.message : String(error),
    });
    process.exitCode = 1;
  } finally {
    store.close();
  }
});
