import type { RecognitionEventInput } from "../src";

export const E2E_SESSION_ID = "session";
export const E2E_SOURCES = ["alpha", "bravo", "charlie"] as const;
export const E2E_SEQUENCE_COUNT = 10;

export function makeE2EEvents(): RecognitionEventInput[] {
  const events: RecognitionEventInput[] = [];

  for (const sourceId of E2E_SOURCES) {
    for (let seq = 0; seq < E2E_SEQUENCE_COUNT; seq += 1) {
      events.push({
        eventId: `${sourceId}-partial-${seq}`,
        sourceId,
        sourceSeq: seq,
        isFinal: false,
        text: `${sourceId}:partial:${seq} `,
      });
      events.push({
        eventId: `${sourceId}-final-${seq}`,
        sourceId,
        sourceSeq: seq,
        isFinal: true,
        text: `${sourceId}:final:${seq} `,
      });
    }
  }

  return events;
}

export function shuffleE2EEvents(
  events: RecognitionEventInput[],
  seed: number
): RecognitionEventInput[] {
  const result = [...events];
  let value = seed >>> 0;

  for (let i = result.length - 1; i > 0; i -= 1) {
    value = (value * 1664525 + 1013904223) >>> 0;
    const j = value % (i + 1);
    [result[i], result[j]] = [result[j] as RecognitionEventInput, result[i] as RecognitionEventInput];
  }

  return result;
}

export function expectedE2EFinalText(): string {
  return E2E_SOURCES.flatMap((sourceId) =>
    Array.from({ length: E2E_SEQUENCE_COUNT }, (_, seq) => `${sourceId}:final:${seq} `)
  ).join("");
}
