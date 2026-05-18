/**
 * Per-(account, sequence) in-flight dispatch budget.
 *
 * Why this exists: the per-wake dispatch budget (MAX_DISPATCHES_PER_WAKE)
 * caps how many jobs one wake can emit, but it doesn't bound how many of
 * a sequence's jobs can sit in one account's BullMQ queue at any moment.
 * When campaign A has hundreds of completed step_results firing Realtime
 * events, it wakes at the worker's drain rate and floods the queue with
 * its own dispatches faster than they drain — campaign B sharing the same
 * account never gets a fair share of queue depth.
 *
 * Solution: hard-cap the number of in-flight jobs per (account, sequence)
 * via a Redis counter. Orchestrator INCRs before each dispatch and refuses
 * to dispatch if it would push the count over the cap. Worker DECRs when
 * the job completes. Multiple sequences sharing one account all bump up
 * against the same cap, so the queue stays balanced regardless of which
 * sequence's wakes fire faster.
 *
 * Backed by the BullMQ Redis connection (already battle-tested at our
 * volume), with 24h TTL on each key as a safety net against counter
 * desync from abandoned worker processes.
 */

import { connection } from '../queues/definitions';

/**
 * Maximum number of jobs from one sequence allowed in one account's BullMQ
 * queue at any moment.
 *
 * Lower values give better fairness across sequences sharing one account.
 * The mechanism: worker drains the per-account queue FIFO; when a job
 * completes the orchestrator wakes and refills *to the back* of the
 * queue. A small cap means each sequence's burst occupies a short
 * window in the queue before the next sequence's chunk takes over.
 *
 * Tuning at cap=10 with 17/min worker pacing:
 *   - 10 jobs per sequence ≈ 35 sec of queue runway
 *   - 3 sequences sharing an account → ~30-job rotating queue → full
 *     round-robin cycle every ~1.7 min
 *   - Much higher and one sequence with a head-start can dominate the
 *     FIFO front for many minutes (see staging stress-test of 3
 *     staggered campaigns where cap=30 starved campaign C for >9 min)
 *   - Much lower (e.g. 3) risks underutilising the worker if wake
 *     events lag the queue drain, leaving brief idle windows
 */
const MAX_INFLIGHT_PER_SEQ_PER_ACCT = 10;

const TTL_SECONDS = 86_400; // 24h safety expiry: abandoned counters reset daily

const key = (acctId: string, seqId: string): string =>
  `dispatch:inflight:${acctId}:${seqId}`;

/**
 * Atomically reserve a dispatch slot for (account, sequence). Returns true
 * if reserved (caller must dispatch), false if at cap (caller must skip).
 *
 * If the counter doesn't exist, INCR creates it at 1 and we set the TTL.
 * On subsequent calls within the TTL window, INCR just bumps the count.
 * The TTL refreshes naturally on each first-of-day reset.
 */
export async function tryReserveDispatchSlot(
  acctId: string,
  seqId: string,
): Promise<boolean> {
  const k = key(acctId, seqId);
  const count = await connection.incr(k);
  if (count > MAX_INFLIGHT_PER_SEQ_PER_ACCT) {
    // Over the cap. Undo and refuse.
    await connection.decr(k);
    // Log once every ~100 refusals per (account, sequence) so we can spot
    // counter desync without flooding logs. Modulo on the raw INCR'd count
    // gives us a sparse heartbeat.
    if (count % 100 === 0) {
      console.warn(
        `[dispatch-budget] persistent refusal acct=${acctId} seq=${seqId} count=${count} (cap=${MAX_INFLIGHT_PER_SEQ_PER_ACCT}). ` +
          `If sequence isn't sending, worker may have crashed mid-flight or counter is desynced; ` +
          `key expires in <=${TTL_SECONDS}s.`,
      );
    }
    return false;
  }
  if (count === 1) {
    // Fresh key — set the safety TTL.
    await connection.expire(k, TTL_SECONDS);
  }
  return true;
}

/**
 * Release a dispatch slot. Called by the worker on job completion (success
 * OR final failure). Uses a small Lua script to ensure we don't decrement
 * below zero — defensive against any counter desync between the orchestrator
 * and the worker (e.g., from out-of-order events, lost reservations).
 */
const DECR_NOT_BELOW_ZERO = `
  local v = tonumber(redis.call('GET', KEYS[1]) or '0')
  if v > 0 then return redis.call('DECR', KEYS[1]) end
  return 0
`;

export async function releaseDispatchSlot(
  acctId: string,
  seqId: string,
): Promise<void> {
  await connection.eval(DECR_NOT_BELOW_ZERO, 1, key(acctId, seqId));
}

/** Diagnostic: current in-flight count for (account, sequence). */
export async function getInflightCount(
  acctId: string,
  seqId: string,
): Promise<number> {
  const v = await connection.get(key(acctId, seqId));
  return v ? Number(v) : 0;
}

/**
 * Clear every `dispatch:inflight:*` key in Redis. Called on orchestrator
 * startup so we don't carry over orphan reservations from a previous
 * process that crashed mid-dispatch (orchestrator INCRs the counter
 * before the BullMQ enqueue completes, so a crash window can leak
 * reservations that nothing will ever release).
 *
 * Brief side-effect: real in-flight jobs from the previous orchestrator
 * session still exist in BullMQ; when they complete the worker calls
 * releaseDispatchSlot, which is a clamped DECR (won't go below zero) —
 * harmless. Worst case is a few seconds of slightly over-dispatch
 * before the new counter values catch up to reality, which is well
 * within the cap's design tolerance.
 */
export async function clearAllInflightCounters(): Promise<number> {
  const stream = connection.scanStream({ match: 'dispatch:inflight:*', count: 200 });
  let total = 0;
  return new Promise<number>((resolve, reject) => {
    stream.on('data', async (keys: string[]) => {
      if (keys.length === 0) return;
      stream.pause();
      try {
        await connection.del(...keys);
        total += keys.length;
      } catch (err) {
        stream.destroy(err as Error);
        return;
      }
      stream.resume();
    });
    stream.on('end', () => resolve(total));
    stream.on('error', reject);
  });
}

/** Tunable for tests / future scale work. */
export { MAX_INFLIGHT_PER_SEQ_PER_ACCT };
