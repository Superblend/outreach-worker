import { Queue, QueueEvents } from 'bullmq';
import IORedis from 'ioredis';
import { config } from '../config';

export const connection = new IORedis(config.redis.url, {
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
});

// Main queues
export const executionQueue = new Queue('outreach-executions', { connection });
export const batchQueue = new Queue('outreach-batches', { connection });
export const scannerQueue = new Queue('outreach-scanner', { connection });
export const recoveryQueue = new Queue('outreach-recovery', { connection });

/**
 * Orchestrator → router pipeline (two-tier dispatch).
 *
 * The orchestrator pushes decisions here without making queue-routing choices.
 * The router consumer in the worker process pulls, resolves step_type + account
 * assignment from the DB, and re-enqueues to the appropriate per-account
 * (LinkedIn) or per-client (Email) queue.
 *
 * Keeping queue topology in the worker is the doc's "worker owns queue
 * topology" rule. Orchestrator stays pure: timing + slot + cohort only.
 */
export const dispatchPendingQueue = new Queue('outreach-dispatch-pending', { connection });

// Queue events for monitoring
export const executionEvents = new QueueEvents('outreach-executions', { connection });
export const batchEvents = new QueueEvents('outreach-batches', { connection });

export const allQueues = [executionQueue, batchQueue, scannerQueue, recoveryQueue];

// Per-account queue factory — returns a cached Queue for outreach-linkedin-{id} / outreach-email-{id}
const accountQueueCache = new Map<string, Queue>();

export function getAccountQueue(queueName: string): Queue {
  if (!accountQueueCache.has(queueName)) {
    accountQueueCache.set(queueName, new Queue(queueName, { connection }));
  }
  return accountQueueCache.get(queueName)!;
}

export { accountQueueCache };
