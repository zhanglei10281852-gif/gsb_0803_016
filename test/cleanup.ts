import { rmSync } from 'node:fs';

export function forceRemove(path: string, maxAttempts = 5): void {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      rmSync(path, { recursive: true, force: true });
      return;
    } catch {
      const wait = 100 * (attempt + 1);
      const end = Date.now() + wait;
      while (Date.now() < end) {
        // busy-wait briefly; Atomics.wait would be overkill here
      }
    }
  }
  try {
    rmSync(path, { recursive: true, force: true });
  } catch {
    // best effort
  }
}
