import { createRecognitionStore } from "../src";
import { E2E_SESSION_ID, makeE2EEvents } from "./e2e-scenarios";

const filename = process.env.E2E_DB;
if (!filename) {
  process.exit(2);
}

const store = createRecognitionStore({ filename, busyTimeout: 15000 });
const waitBuffer = new Int32Array(new SharedArrayBuffer(4));

try {
  for (const event of makeE2EEvents()) {
    store.ingest(E2E_SESSION_ID, event);
    Atomics.wait(waitBuffer, 0, 0, Math.floor(Math.random() * 6));
  }
} finally {
  store.close();
}
