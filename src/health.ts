import { Worker } from 'bullmq';

export interface WorkerHealth {
  lastJobCompletedAt: Date | null;
  lastScannerRunAt: Date | null;
  worker: Worker | null;
  workerRestarting: boolean;
}

export const workerHealth: WorkerHealth = {
  lastJobCompletedAt: null,
  lastScannerRunAt: null,
  worker: null,
  workerRestarting: false,
};
