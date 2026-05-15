import { connection, getAccountQueue } from './queues/definitions';
import { createAccountWorker } from './workers/execution.worker';
import { Worker } from 'bullmq';

// A2: Reconcile every 60s (was 30s). Binds a consumer to every queue that
// exists in Redis regardless of current job count — delayed-only queues need a
// consumer just as much as queues with waiting jobs.
const RECONCILE_INTERVAL_MS = 60_000;
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
   * Called directly by the scanner after each enqueue — primary mechanism.
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

  /**
   * Scan Redis for all account queues; bind a consumer to each regardless of
   * job count (A2 fix). Separately computes hasPendingJobs for the watchdog.
   */
  async reconcile(): Promise<void> {
    console.log(`🔄 WorkerManager: reconcile starting (active workers=${this.workers.size})`);
    try {
      // A2: scan all queues that exist in Redis (not just those with pending jobs)
      const allQueues = await this.scanAllQueueNames();
      console.log(`🔄 WorkerManager: found ${allQueues.size} queue(s) in Redis: ${[...allQueues].slice(0, 5).join(', ')}${allQueues.size > 5 ? '…' : ''}`);

      // Bind a consumer to every queue. Delayed-only queues need a consumer too.
      for (const queueName of allQueues) {
        if (this.workers.has(queueName)) continue;
        const parsed = parseAccountQueue(queueName);
        if (!parsed) continue;
        const { channel, entityId } = parsed;
        try {
          const q = getAccountQueue(queueName);
          const [w, d] = await Promise.allSettled([q.getWaitingCount(), q.getDelayedCount()])
            .then(([wr, dr]) => [
              wr.status === 'fulfilled' ? wr.value : 0,
              dr.status === 'fulfilled' ? dr.value : 0,
            ]);
          console.log(`[watchdog] rebinding consumer for queue=${queueName} waiting=${w} delayed=${d} before rebind`);
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

      // Close workers whose queues have vanished from Redis AND been idle > 5 min.
      for (const [queueName, lastActive] of this.workerLastActive) {
        if (allQueues.has(queueName)) continue;
        if (Date.now() - lastActive < WORKER_IDLE_TIMEOUT_MS) continue;
        const worker = this.workers.get(queueName);
        if (worker) {
          try { await worker.close(); } catch {}
          this.workers.delete(queueName);
          this.workerLastActive.delete(queueName);
          console.log(`🔧 WorkerManager: closed idle worker for ${queueName}`);
        }
      }

      // Compute hasPendingJobs separately so the watchdog stays accurate
      // even though we now bind workers to empty queues.
      let totalPending = 0;
      for (const queueName of this.workers.keys()) {
        try {
          const q = getAccountQueue(queueName);
          const [w, a, d] = await Promise.all([q.getWaitingCount(), q.getActiveCount(), q.getDelayedCount()]);
          totalPending += w + a + d;
        } catch { /* ignore — queue may be transitioning */ }
      }
      this.hasPendingJobs = totalPending > 0;

      console.log(`🔄 WorkerManager: reconcile complete (active workers=${this.workers.size} pending=${totalPending})`);
    } catch (err: any) {
      console.error('WorkerManager: reconcile top-level error:', err.stack ?? err.message);
    }
  }

  /**
   * Scan Redis for all outreach-linkedin-* and outreach-email-client-* queue names.
   * Does NOT filter by job count — returns every queue that has ever been created.
   */
  private async scanAllQueueNames(): Promise<Set<string>> {
    const found = new Set<string>();
    const prefixes = ['bull:outreach-linkedin-', 'bull:outreach-email-client-'];

    for (const prefix of prefixes) {
      const pattern = `${prefix}*:id`;
      let keys: string[] = [];

      try {
        keys = await this.scanKeys(pattern);
        console.log(`WorkerManager: SCAN "${pattern}" → ${keys.length} key(s)`);
      } catch (scanErr: any) {
        try {
          keys = await connection.keys(pattern);
          console.log(`WorkerManager: KEYS fallback "${pattern}" → ${keys.length} key(s)`);
        } catch (keysErr: any) {
          console.error(`WorkerManager: both SCAN and KEYS failed for ${prefix}:`, keysErr.message);
          continue;
        }
      }

      for (const key of keys) {
        const match = key.match(/^bull:(outreach-(?:linkedin|email-client)-.+):id$/);
        if (match) found.add(match[1]);
        else console.warn(`WorkerManager: unexpected key format skipped: ${key}`);
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

  /** Drain all per-account queues. Called ONLY on process startup to clear stale session jobs. */
  async drainAllAccountQueues(): Promise<void> {
    let keys: string[] = [];
    for (const prefix of ['bull:outreach-linkedin-', 'bull:outreach-email-client-']) {
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

  /**
   * Close all account workers then re-reconcile (binds fresh consumers).
   * A1 fix: does NOT drain queues — waiting/delayed jobs are valid and must survive restart.
   */
  async restartAll(): Promise<void> {
    console.log('🔄 WorkerManager: restarting all account workers...');
    for (const [queueName, worker] of this.workers) {
      try {
        const q = getAccountQueue(queueName);
        const [w, d] = await Promise.allSettled([q.getWaitingCount(), q.getDelayedCount()])
          .then(([wr, dr]) => [
            wr.status === 'fulfilled' ? wr.value : 0,
            dr.status === 'fulfilled' ? dr.value : 0,
          ]);
        console.log(`[watchdog] rebinding consumer for queue=${queueName} waiting=${w} delayed=${d} before rebind`);
        await worker.close();
      } catch (e: any) {
        console.error(`WorkerManager: error closing ${queueName}:`, e.message);
      }
    }
    this.workers.clear();
    this.workerLastActive.clear();
    // Do NOT drain — jobs remain in queues and need a fresh consumer, not deletion.
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
