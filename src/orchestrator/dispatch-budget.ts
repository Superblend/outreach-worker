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
 * queue at any moment. Tuned for:
 *   - per-account email worker pacing (~17/min) means 30 jobs ≈ 100 sec
 *     of queue runway per sequence, comfortably above the orchestrator's
 *     30s pull cooldown so we never deadlock
 *   - small enough that 5 sequences on one account = 150 max queue depth,
 *     low BullMQ overhead
 *   - large enough to absorb a wake's full batch (MAX_DISPATCHES_PER_WAKE = 10)
 *     three times before saturating
 */
const MAX_INFLIGHT_PER_SEQ_PER_ACCT = 30;

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

/** Tunable for tests / future scale work. */
export { MAX_INFLIGHT_PER_SEQ_PER_ACCT };
