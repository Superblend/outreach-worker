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
      () => this.reconcile().catch(console.error),
      RECONCILE_INTERVAL_MS,
    );
    console.log(`✅ WorkerManager started (reconcile every ${RECONCILE_INTERVAL_MS / 1000}s)`);
  }

  /** Scan Redis for account queues with pending jobs; create/close workers accordingly. */
  async reconcile(): Promise<void> {
    try {
      const activeQueues = await this.scanActiveQueues();
      this.hasPendingJobs = activeQueues.size > 0;

      // Create workers for queues that appeared since last reconcile
      for (const queueName of activeQueues) {
        if (!this.workers.has(queueName)) {
          const parsed = parseAccountQueue(queueName);
          if (!parsed) continue;
          const { channel, accountId } = parsed;
          const worker = createAccountWorker(queueName, channel, accountId, () => {
            this.workerLastActive.set(queueName, Date.now());
          });
          this.workers.set(queueName, worker);
          this.workerLastActive.set(queueName, Date.now());
          console.log(`🔧 WorkerManager: created ${channel} worker for ${accountId} (queue=${queueName})`);
        }
      }

      // Close workers whose queues have been empty for 5+ minutes
      for (const [queueName, lastActive] of this.workerLastActive) {
        if (activeQueues.has(queueName)) continue;
        if (Date.now() - lastActive < WORKER_IDLE_TIMEOUT_MS) continue;
        const worker = this.workers.get(queueName);
        if (worker) {
          await worker.close();
          this.workers.delete(queueName);
          this.workerLastActive.delete(queueName);
          console.log(`🔧 WorkerManager: closed idle worker for ${queueName}`);
        }
      }
    } catch (err: any) {
      console.error('WorkerManager: reconcile error:', err.message);
    }
  }

  /** Scan Redis for outreach-linkedin-* and outreach-email-* queues that have pending jobs. */
  private async scanActiveQueues(): Promise<Set<string>> {
    const found = new Set<string>();
    const patterns = ['bull:outreach-linkedin-*:id', 'bull:outreach-email-*:id'];

    for (const pattern of patterns) {
      let keys: string[];
      try {
        keys = await connection.keys(pattern);
      } catch {
        continue;
      }

      for (const key of keys) {
        const match = key.match(/^bull:(.+):id$/);
        if (!match) continue;
        const queueName = match[1];
        try {
          const q = getAccountQueue(queueName);
          const [waiting, active, delayed] = await Promise.all([
            q.getWaitingCount(),
            q.getActiveCount(),
            q.getDelayedCount(),
          ]);
          if (waiting + active + delayed > 0) {
            found.add(queueName);
          }
        } catch {
          // Queue may have been removed — skip
        }
      }
    }

    return found;
  }

  /** Drain all per-account queues. Called on startup to clear stale session jobs. */
  async drainAllAccountQueues(): Promise<void> {
    const patterns = ['bull:outreach-linkedin-*:id', 'bull:outreach-email-*:id'];
    let drained = 0;
    for (const pattern of patterns) {
      const keys = await connection.keys(pattern).catch(() => [] as string[]);
      for (const key of keys) {
        const match = key.match(/^bull:(.+):id$/);
        if (!match) continue;
        try {
          await getAccountQueue(match[1]).drain();
          drained++;
        } catch (e: any) {
          console.warn(`WorkerManager: drain error for ${match[1]}:`, e.message);
        }
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
): { channel: 'linkedin' | 'email'; accountId: string } | null {
  if (queueName.startsWith('outreach-linkedin-')) {
    return { channel: 'linkedin', accountId: queueName.slice('outreach-linkedin-'.length) };
  }
  if (queueName.startsWith('outreach-email-')) {
    return { channel: 'email', accountId: queueName.slice('outreach-email-'.length) };
  }
  return null;
}

export const workerManager = new WorkerManager();
