/**
 * Periodic reconciler for the dispatch-budget Redis counters.
 *
 * The counters can drift out of sync with reality when the orchestrator
 * INCRs but the corresponding worker release doesn't fire — most often
 * because a dispatched job never reached BullMQ (transient Redis blip,
 * process crash during enqueue, deploy race) or because the worker
 * process died before emitting the `completed`/`failed` event. The
 * 24h TTL on each key is the floor safety net, but four hours of a
 * stuck sequence is already a customer-visible failure.
 *
 * This reconciler runs every 60 seconds and, for each non-zero counter:
 *   1. Parses the key into (accountId, sequenceId)
 *   2. Counts actual jobs for that (sequenceId) sitting in either of the
 *      two per-account queues (`outreach-email-acct-{accountId}`
 *      or `outreach-linkedin-{accountId}`), across all live states
 *      (waiting / prioritized / delayed / active)
 *   3. If the Redis counter > actual count, snaps it down to the actual.
 *      Counter < actual is left alone (under-count auto-corrects on next
 *      INCR, never causes over-dispatch).
 *
 * This eliminates the entire "stuck counter, sequence frozen at the cap"
 * failure mode that has bitten the staging stress test multiple times.
 *
 * Cost at scale: scan + N×2 getJobs calls per cycle. With N=200 active
 * (account, sequence) pairs and ~10 jobs per queue at cap, that's ~400
 * Redis ops per minute — well within Redis/BullMQ capacity.
 */

import { connection, getAccountQueue } from '../queues/definitions';
import { listInflightCounters, resetInflightCounter } from './dispatch-budget';

const RECONCILE_INTERVAL_MS = 60_000;
const TTL_SECONDS = 86_400;

let reconcileTimer: NodeJS.Timeout | null = null;

export function startDispatchBudgetReconciler(): void {
  if (reconcileTimer) return;
  reconcileTimer = setInterval(() => {
    runReconcile().catch((err) => {
      console.error('[dispatch-budget-reconcile] cycle error:', (err as Error).message);
    });
  }, RECONCILE_INTERVAL_MS);
  // Initial cycle ~10s after startup, after Realtime + poll are wired up.
  setTimeout(() => {
    runReconcile().catch(() => undefined);
  }, 10_000);
  console.log(`✅ Dispatch-budget reconciler started (every ${RECONCILE_INTERVAL_MS / 1000}s)`);
}

export function stopDispatchBudgetReconciler(): void {
  if (reconcileTimer) {
    clearInterval(reconcileTimer);
    reconcileTimer = null;
  }
}

/**
 * Single reconciliation cycle. Public for tests / one-shot triggers via
 * the admin endpoint if needed.
 */
export async function runReconcile(): Promise<{
  examined: number;
  corrected: number;
  totalCounter: number;
  totalActual: number;
}> {
  const counters = await listInflightCounters();
  let corrected = 0;
  let totalCounter = 0;
  let totalActual = 0;

  for (const { key, count } of counters) {
    if (count <= 0) continue;

    // Key format: dispatch:inflight:{accountId}:{sequenceId}
    const parts = key.split(':');
    if (parts.length !== 4) continue;
    const acctId = parts[2];
    const seqId = parts[3];

    // An account can be LinkedIn or email. We don't track which from the
    // counter key alone, so check both possible queue names; non-existent
    // queues just return empty job lists.
    let actualCount = 0;
    for (const queueName of [
      `outreach-email-acct-${acctId}`,
      `outreach-linkedin-${acctId}`,
    ]) {
      try {
        const queue = getAccountQueue(queueName);
        const jobs = await queue.getJobs(['waiting', 'prioritized', 'delayed', 'active']);
        actualCount += jobs.filter((j) => j.data?.sequence_id === seqId).length;
      } catch {
        // Queue doesn't exist or transient error — treat as 0 for this queue.
      }
    }

    totalCounter += count;
    totalActual += actualCount;

    if (count > actualCount) {
      try {
        if (actualCount === 0) {
          await resetInflightCounter(acctId, seqId);
        } else {
          await connection.set(key, actualCount.toString());
          await connection.expire(key, TTL_SECONDS);
        }
        corrected++;
        console.warn(
          `[dispatch-budget-reconcile] corrected acct=${acctId.slice(0, 8)} seq=${seqId.slice(0, 8)} counter=${count} → ${actualCount}`,
        );
      } catch (err) {
        console.error(
          `[dispatch-budget-reconcile] failed to correct ${key}:`,
          (err as Error).message,
        );
      }
    }
  }

  if (corrected > 0) {
    console.log(
      `[dispatch-budget-reconcile] cycle: examined=${counters.length} corrected=${corrected} ` +
        `total_counter=${totalCounter} actual=${totalActual}`,
    );
  }

  return { examined: counters.length, corrected, totalCounter, totalActual };
}
