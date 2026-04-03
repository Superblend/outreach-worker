import express from 'express';
import { config } from './config';
import { startScanner } from './scanner';
import { startExecutionWorker } from './workers/execution.worker';
import { startBatchWorker } from './workers/batch.worker';
import { setupBullBoard } from './monitoring/bull-board';
import { connection, executionQueue } from './queues/definitions';
import { workerHealth } from './health';

const WORKER_STALE_MS = 5 * 60_000; // 5 minutes
const SCANNER_STALE_MS = 60_000;    // 60 seconds
let watchdogRunning = false;

async function runWatchdog() {
  if (watchdogRunning) return;
  watchdogRunning = true;
  try {
    // Don't check until the process has had enough time to warm up
    if (process.uptime() * 1000 < WORKER_STALE_MS) return;

    const lastCompleted = workerHealth.lastJobCompletedAt?.getTime() ?? 0;
    const isStale = lastCompleted === 0 || Date.now() - lastCompleted > WORKER_STALE_MS;
    if (!isStale || workerHealth.workerRestarting) return;

    let pending = 0;
    try {
      const [waiting, active, delayed] = await Promise.all([
        executionQueue.getWaitingCount(),
        executionQueue.getActiveCount(),
        executionQueue.getDelayedCount(),
      ]);
      pending = waiting + active + delayed;
    } catch {
      return; // Redis issue — can't check
    }

    if (pending === 0) return;

    const staleSeconds = lastCompleted
      ? Math.round((Date.now() - lastCompleted) / 1000)
      : Math.round(process.uptime());
    console.error(`🚨 CRITICAL: Worker consumer stale for ${staleSeconds}s with ${pending} pending jobs — restarting consumer`);

    workerHealth.workerRestarting = true;
    try {
      if (workerHealth.worker) {
        await workerHealth.worker.close();
        workerHealth.worker = null;
      }
      workerHealth.lastJobCompletedAt = null; // reset so watchdog doesn't re-fire immediately
      startExecutionWorker();
      console.log('✅ Worker consumer restarted by watchdog');
    } catch (err: any) {
      console.error('❌ Watchdog failed to restart worker consumer:', err.message);
    } finally {
      workerHealth.workerRestarting = false;
    }
  } finally {
    watchdogRunning = false;
  }
}

async function main() {
  console.log('🚀 Outreach Worker starting...');
  console.log(`   Supabase: ${config.supabase.url}`);
  console.log(`   Redis: ${config.redis.url.replace(/\/\/.*@/, '//***@')}`);

  // Redis connection event logging
  connection.on('connect', () => console.log('✅ Redis connection established'));
  connection.on('close', () => console.error('❌ Redis connection closed'));
  connection.on('reconnecting', (delay: number) => console.warn(`⚠️ Redis reconnecting in ${delay}ms...`));
  connection.on('error', (err: Error) => console.error('❌ Redis error:', err.message));

  // Verify Redis connection
  await connection.ping();
  console.log('✅ Redis connected');

  // Start workers
  startExecutionWorker();
  startBatchWorker();
  await startScanner();

  // Watchdog: restart worker consumer if it stops processing jobs
  setInterval(runWatchdog, 60_000);

  // HTTP server for health checks + Bull Board
  const app = express();

  app.get('/health', async (_req, res) => {
    const now = Date.now();
    const lastCompleted = workerHealth.lastJobCompletedAt?.getTime() ?? 0;
    const workerStale = process.uptime() * 1000 > WORKER_STALE_MS &&
      (lastCompleted === 0 || now - lastCompleted > WORKER_STALE_MS);
    const scannerStale = !workerHealth.lastScannerRunAt ||
      now - workerHealth.lastScannerRunAt.getTime() > SCANNER_STALE_MS;

    let pending = 0;
    let redisOk = true;
    try {
      const [waiting, active, delayed] = await Promise.all([
        executionQueue.getWaitingCount(),
        executionQueue.getActiveCount(),
        executionQueue.getDelayedCount(),
      ]);
      pending = waiting + active + delayed;
    } catch {
      redisOk = false;
    }

    const workerUnhealthy = workerStale && pending > 0;
    const healthy = redisOk && !workerUnhealthy && !scannerStale;

    res.status(healthy ? 200 : 503).json({
      status: healthy ? 'ok' : 'unhealthy',
      uptime: Math.round(process.uptime()),
      worker: {
        lastJobCompletedAt: workerHealth.lastJobCompletedAt,
        stale: workerStale,
        restarting: workerHealth.workerRestarting,
      },
      scanner: {
        lastRunAt: workerHealth.lastScannerRunAt,
        stale: scannerStale,
      },
      queue: { pending, redisOk },
    });
  });

  // Bull Board dashboard
  const bullBoardAdapter = setupBullBoard();
  app.use('/admin/queues', bullBoardAdapter.getRouter());

  app.listen(config.port, () => {
    console.log(`✅ HTTP server on port ${config.port}`);
    console.log(`   Health: http://localhost:${config.port}/health`);
    console.log(`   Dashboard: http://localhost:${config.port}/admin/queues`);
  });
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
