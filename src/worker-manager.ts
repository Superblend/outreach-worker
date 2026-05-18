import { connection, getAccountQueue } from './queues/definitions';
import { createAccountWorker } from './workers/execution.worker';
import { Worker } from 'bullmq';

const RECONCILE_INTERVAL_MS = 30_000;
// After a queue has been empty for this long, close its worker to free resources
const WORKER_IDLE_TIMEOUT_MS = 5 * 60_000;

class WorkerManager {
  private workers = new Map<string, Worker>();
  // Tracks last time a job completed (or worker was first created) per queue
  private workerLastActive = new Map<string, number>();
  private reconcileTimer?: NodeJS.Timeout;
  // Set by reconcile(); used by the watchdog to check if there's pending work
  hasPendingJobs = false;

  async start(): Promise<void> {
    await this.reconcile();
    this.reconcileTimer = setInterval(
      () => this.reconcile().catch((err: any) =>
        console.error('WorkerManager: reconcile interval error:', err.message),
      ),
      RECONCILE_INTERVAL_MS,
    );
    console.log(`✅ WorkerManager started (reconcile every ${RECONCILE_INTERVAL_MS / 1000}s)`);
  }

  /**
   * Ensure a worker exists for the given account queue.
   * Called directly by the scanner after each enqueue — does not depend on
   * Redis key discovery, so it works regardless of KEYS/SCAN availability.
   */
  ensureWorker(queueName: string): void {
    if (this.workers.has(queueName)) return;
    const parsed = parseAccountQueue(queueName);
    if (!parsed) {
      console.warn(`WorkerManager: ensureWorker called with unrecognised queue name: ${queueName}`);
      return;
    }
    const { channel, entityId } = parsed;
    const worker = createAccountWorker(queueName, channel, entityId, () => {
      this.workerLastActive.set(queueName, Date.now());
    });
    this.workers.set(queueName, worker);
    this.workerLastActive.set(queueName, Date.now());
    this.hasPendingJobs = true;
    console.log(`🔧 WorkerManager: created ${channel} worker for ${entityId} (queue=${queueName}) [via ensureWorker]`);
  }

  /** Scan Redis for account queues with pending jobs; create/close workers accordingly. */
  async reconcile(): Promise<void> {
    console.log(`🔄 WorkerManager: reconcile starting (active workers=${this.workers.size})`);
    try {
      const activeQueues = await this.scanActiveQueues();
      this.hasPendingJobs = activeQueues.size > 0;
      console.log(`🔄 WorkerManager: reconcile found ${activeQueues.size} active queue(s): ${[...activeQueues].join(', ') || '(none)'}`);

      // Create workers for queues that appeared since last reconcile
      for (const queueName of activeQueues) {
        if (!this.workers.has(queueName)) {
          const parsed = parseAccountQueue(queueName);
          if (!parsed) continue;
          const { channel, entityId } = parsed;
          try {
            const worker = createAccountWorker(queueName, channel, entityId, () => {
              this.workerLastActive.set(queueName, Date.now());
            });
            this.workers.set(queueName, worker);
            this.workerLastActive.set(queueName, Date.now());
            console.log(`🔧 WorkerManager: created ${channel} worker for ${entityId} (queue=${queueName})`);
          } catch (err: any) {
            console.error(`WorkerManager: failed to create worker for ${queueName}:`, err.message);
          }
        }
      }

      // Close workers whose queues have been empty for 5+ minutes
      for (const [queueName, lastActive] of this.workerLastActive) {
        if (activeQueues.has(queueName)) continue;
        if (this.workers.has(queueName) && Date.now() - lastActive < WORKER_IDLE_TIMEOUT_MS) continue;
        const worker = this.workers.get(queueName);
        if (worker) {
          try {
            await worker.close();
          } catch (err: any) {
            console.warn(`WorkerManager: error closing worker for ${queueName}:`, err.message);
          }
          this.workers.delete(queueName);
          this.workerLastActive.delete(queueName);
          console.log(`🔧 WorkerManager: closed idle worker for ${queueName}`);
        }
      }

      console.log(`🔄 WorkerManager: reconcile complete (active workers=${this.workers.size})`);
    } catch (err: any) {
      console.error('WorkerManager: reconcile top-level error:', err.stack ?? err.message);
    }
  }

  /**
   * Scan Redis for outreach-linkedin-* and outreach-email-* queues that have pending jobs.
   * Uses SCAN (cursor-based) instead of KEYS so it works on managed Redis providers
   * (Upstash, Railway internal Redis) that disable the blocking KEYS command.
   *
   * Both `outreach-email-acct-*` (per-account, current model from Phase 2)
   * and `outreach-email-client-*` (legacy per-client; kept for backward
   * compatibility while pre-Phase-2 jobs still drain) are recognised.
   */
  private async scanActiveQueues(): Promise<Set<string>> {
    const found = new Set<string>();
    const prefixes = [
      'bull:outreach-linkedin-',
      'bull:outreach-email-acct-',
      'bull:outreach-email-client-', // legacy, drains in-flight jobs only
    ];

    for (const prefix of prefixes) {
      const pattern = `${prefix}*:id`;
      let keys: string[] = [];

      // Try SCAN first (non-blocking, works everywhere)
      try {
        keys = await this.scanKeys(pattern);
        console.log(`WorkerManager: SCAN "${pattern}" → ${keys.length} key(s): ${keys.slice(0, 5).join(', ')}${keys.length > 5 ? '…' : ''}`);
      } catch (scanErr: any) {
        console.error(`WorkerManager: SCAN failed for pattern "${pattern}":`, scanErr.message);
        // Fall back to KEYS if SCAN is somehow unavailable
        try {
          keys = await connection.keys(pattern);
          console.log(`WorkerManager: KEYS fallback "${pattern}" → ${keys.length} key(s)`);
        } catch (keysErr: any) {
          console.error(`WorkerManager: KEYS also failed for pattern "${pattern}":`, keysErr.message);
          continue;
        }
      }

      for (const key of keys) {
        // Key format: bull:outreach-linkedin-{accountId}:id,
        //             bull:outreach-email-acct-{accountId}:id,
        //          or bull:outreach-email-client-{clientId}:id (legacy)
        const match = key.match(/^bull:(outreach-(?:linkedin|email-acct|email-client)-.+):id$/);
        if (!match) {
          console.warn(`WorkerManager: unexpected key format skipped: ${key}`);
          continue;
        }
        const queueName = match[1];
        try {
          const q = getAccountQueue(queueName);
          const [waiting, active, delayed, prioritized] = await Promise.all([
            q.getWaitingCount(),
            q.getActiveCount(),
            q.getDelayedCount(),
            // Prioritized count — jobs added with `priority` option go to a
            // separate sorted set, NOT the waiting list. The orchestrator
            // always sets priority (cohort priority); without this check the
            // worker manager treats those queues as empty and never spawns a
            // consumer. getPrioritizedCount() returns a Promise<number>.
            q.getPrioritizedCount(),
          ]);
          const total = waiting + active + delayed + prioritized;
          console.log(
            `WorkerManager: queue ${queueName} → waiting=${waiting} active=${active} ` +
              `delayed=${delayed} prioritized=${prioritized}`,
          );
          if (total > 0) {
            found.add(queueName);
          }
        } catch (err: any) {
          console.warn(`WorkerManager: could not check counts for ${queueName}:`, err.message);
        }
      }
    }

    return found;
  }

  /** Cursor-based SCAN — O(1) per call, safe on all Redis providers. */
  private async scanKeys(pattern: string): Promise<string[]> {
    const results: string[] = [];
    let cursor = '0';
    do {
      const [nextCursor, batch] = await connection.scan(cursor, 'MATCH', pattern, 'COUNT', 200);
      results.push(...batch);
      cursor = nextCursor;
    } while (cursor !== '0');
    return results;
  }

  /** Drain all per-account queues. Called on startup to clear stale session jobs. */
  async drainAllAccountQueues(): Promise<void> {
    let keys: string[] = [];
    for (const prefix of [
      'bull:outreach-linkedin-',
      'bull:outreach-email-acct-',
      'bull:outreach-email-client-',
    ]) {
      try {
        const batch = await this.scanKeys(`${prefix}*:id`);
        keys.push(...batch);
      } catch (err: any) {
        console.warn(`WorkerManager: drainAllAccountQueues scan error for ${prefix}:`, err.message);
      }
    }

    let drained = 0;
    for (const key of keys) {
      const match = key.match(/^bull:(outreach-(?:linkedin|email-client)-.+):id$/);
      if (!match) continue;
      try {
        await getAccountQueue(match[1]).drain();
        drained++;
      } catch (e: any) {
        console.warn(`WorkerManager: drain error for ${match[1]}:`, e.message);
      }
    }
    if (drained > 0) {
      console.log(`🧹 WorkerManager: drained ${drained} account queue(s)`);
    }
  }

  /** Close all workers, drain their queues, then re-reconcile. Used by watchdog restart. */
  async restartAll(): Promise<void> {
    console.log('🔄 WorkerManager: restarting all account workers...');
    for (const [queueName, worker] of this.workers) {
      try {
        await worker.close();
      } catch (e: any) {
        console.error(`WorkerManager: error closing ${queueName}:`, e.message);
      }
    }
    this.workers.clear();
    this.workerLastActive.clear();
    await this.drainAllAccountQueues();
    await this.reconcile();
    console.log('✅ WorkerManager: restart complete');
  }

  getActiveWorkerCount(): number {
    return this.workers.size;
  }

  async getWorkerStats(): Promise<Array<{ queueName: string; pending: number; active: number }>> {
    const stats: Array<{ queueName: string; pending: number; active: number }> = [];
    for (const queueName of this.workers.keys()) {
      try {
        const q = getAccountQueue(queueName);
        const [waiting, active, delayed] = await Promise.all([
          q.getWaitingCount(),
          q.getActiveCount(),
          q.getDelayedCount(),
        ]);
        stats.push({ queueName, pending: waiting + delayed, active });
      } catch {
        stats.push({ queueName, pending: -1, active: -1 });
      }
    }
    return stats;
  }

  stop(): void {
    if (this.reconcileTimer) clearInterval(this.reconcileTimer);
  }
}

function parseAccountQueue(
  queueName: string,
): { channel: 'linkedin' | 'email'; entityId: string } | null {
  if (queueName.startsWith('outreach-linkedin-')) {
    return { channel: 'linkedin', entityId: queueName.slice('outreach-linkedin-'.length) };
  }
  if (queueName.startsWith('outreach-email-client-')) {
    return { channel: 'email', entityId: queueName.slice('outreach-email-client-'.length) };
  }
  return null;
}

export const workerManager = new WorkerManager();
