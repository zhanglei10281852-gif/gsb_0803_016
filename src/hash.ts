import { createHash } from 'node:crypto';
import type { IngestEvent } from './types';

export function hashEvent(event: IngestEvent): string {
  const h = createHash('sha256');
  h.update(event.sourceId);
  h.update('\0');
  h.update(String(event.sourceSeq));
  h.update('\0');
  h.update(event.type);
  h.update('\0');
  h.update(event.content);
  return h.digest('hex');
}
