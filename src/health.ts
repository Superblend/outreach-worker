import { Worker } from 'bullmq';

export interface WorkerHealth {
  lastJobCompletedAt: Date | null;
  lastScannerRunAt: Date | null;
  worker: Worker | null;
  workerRestarting: boolean;
  jobsCompletedTotal: number;
  completionTimestamps: number[]; // epoch ms, kept for rolling last-minute count
}

export const workerHealth: WorkerHealth = {
  lastJobCompletedAt: null,
  lastScannerRunAt: null,
  worker: null,
  workerRestarting: false,
  jobsCompletedTotal: 0,
  completionTimestamps: [],
};

/** Call after every job completes (from worker 'completed' event handlers). */
export function recordJobCompletion(): void {
  const now = Date.now();
  workerHealth.lastJobCompletedAt = new Date(now);
  workerHealth.jobsCompletedTotal++;
  workerHealth.completionTimestamps.push(now);
  // Prune entries older than 90s (keeps array bounded; last-minute window is 60s)
  const cutoff = now - 90_000;
  workerHealth.completionTimestamps = workerHealth.completionTimestamps.filter(t => t > cutoff);
}

/** Rolling count of jobs completed in the last 60 seconds. */
export function getLastMinuteJobCount(): number {
  const cutoff = Date.now() - 60_000;
  return workerHealth.completionTimestamps.filter(t => t > cutoff).length;
}
