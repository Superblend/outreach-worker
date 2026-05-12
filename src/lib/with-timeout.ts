export const CALL_TIMEOUT_MS = 30_000;

/**
 * Races a promise against a deadline. On timeout throws Error('TIMEOUT:<tag>:<ms>ms')
 * which BullMQ treats as a retriable error (existing 3-retry policy applies).
 */
export function withTimeout<T>(p: Promise<T>, ms: number, tag: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, rej) =>
      setTimeout(() => rej(new Error(`TIMEOUT:${tag}:${ms}ms`)), ms),
    ),
  ]);
}
