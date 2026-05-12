import express from 'express';
import { config } from './config';
import { startScanner, triggerImmediateScan, SESSION_ID } from './scanner';
import { startExecutionWorker } from './workers/execution.worker';
import { startBatchWorker } from './workers/batch.worker';
import { setupBullBoard } from './monitoring/bull-board';
import { connection, executionQueue } from './queues/definitions';
import { workerHealth, getLastMinuteJobCount } from './health';
import { supabase } from './supabase';
import { workerManager } from './worker-manager';
import { withTimeout } from './lib/with-timeout';

const WORKER_ID = `${process.env.HOSTNAME || 'worker'}-${process.env.WORKER_PARTITION || '0'}`;

async function writeHeartbeat(extra: Record<string, any> = {}): Promise<void> {
  try {
    await withTimeout(
      supabase.from('worker_heartbeats').upsert(
        {
          worker_id: WORKER_ID,
          partition: parseInt(process.env.WORKER_PARTITION || '0', 10),
          last_heartbeat_at: new Date().toISOString(),
          jobs_completed_total: workerHealth.jobsCompletedTotal,
          jobs_completed_last_minute: getLastMinuteJobCount(),
          active_queues: workerManager.getActiveWorkerCount(),
          redis_connected: connection.status === 'ready',
          meta: {
            session: SESSION_ID,
            uptime: Math.round(process.uptime()),
          },
          ...extra,
        },
        { onConflict: 'worker_id' },
      ),
      10_000,
      'supabase:heartbeat',
    );
  } catch (e: any) {
    console.warn(`⚠️ [heartbeat] write failed (non-fatal):`, e.message);
  }
}

// Watchdog fires every 15s; triggers restart after 60s with no completed job + pending work.
const WORKER_STALE_MS = 60_000;
const WATCHDOG_WARMUP_MS = 90_000; // wait for process to fully boot before first stale check
const SCANNER_STALE_MS = 60_000;
let watchdogRunning = false;

// Set to true once all heavy init (Redis, workers, scanner) has completed.
let initialized = false;

async function drainExecutionQueue(): Promise<void> {
  try {
    await executionQueue.drain();
    console.log('🧹 Execution queue drained (waiting + delayed jobs cleared)');
  } catch (err: any) {
    console.error('❌ Failed to drain execution queue:', err.message);
  }
}

async function resetInProgressOrphans(): Promise<void> {
  // On fresh process start, ANY in_progress execution is from a dead previous instance.
  const staleAt = new Date(Date.now() - 31_000).toISOString();
  const { data, error } = await supabase
    .from('unipile_sequence_executions')
    .update({ execution_state: 'not_started', updated_at: staleAt })
    .eq('execution_state', 'in_progress')
    .eq('status', 'running')
    .select('id');
  if (error) {
    console.error('❌ Startup: failed to reset in_progress orphans:', error.message);
  } else {
    console.log(`🔧 Startup: reset ${data?.length ?? 0} in_progress orphan(s) from previous session`);
  }
}

async function runWatchdog() {
  if (watchdogRunning) return;
  watchdogRunning = true;
  try {
    if (process.uptime() * 1000 < WATCHDOG_WARMUP_MS) return;
    if (workerHealth.workerRestarting) return;

    const lastCompleted = workerHealth.lastJobCompletedAt?.getTime() ?? 0;
    const isStale = lastCompleted === 0 || Date.now() - lastCompleted > WORKER_STALE_MS;
    if (!isStale) return;

    let pending = 0;
    try {
      // Check shared queue (delay/conditional steps) + WorkerManager knows account queue state
      const [waiting, active, delayed] = await Promise.all([
        executionQueue.getWaitingCount(),
        executionQueue.getActiveCount(),
        executionQueue.getDelayedCount(),
      ]);
      pending = waiting + active + delayed;
      // Also treat pending account-queue jobs as signal that work exists
      if (workerManager.hasPendingJobs) pending = Math.max(pending, 1);
    } catch {
      return; // Redis issue — skip this cycle
    }

    if (pending === 0) return;

    const staleSeconds = lastCompleted
      ? Math.round((Date.now() - lastCompleted) / 1000)
      : Math.round(process.uptime());
    console.error(`🚨 CRITICAL: Worker consumer stale for ${staleSeconds}s with ${pending} pending jobs — restarting`);

    workerHealth.workerRestarting = true;
    try {
      if (workerHealth.worker) {
        // A1: close without draining — waiting/delayed jobs must survive the restart
        await workerHealth.worker.close();
        workerHealth.worker = null;
      }
      // A1: do NOT drain — drainExecutionQueue() deletes pending jobs whose DB rows
      // remain in running forever. Only drain on fresh process startup (initialize).
      workerHealth.lastJobCompletedAt = null;
      startExecutionWorker();
      await workerManager.restartAll(); // closes + rebinds consumers, no drain
      console.log('✅ Worker consumer restarted by watchdog');
      // Kick off an immediate scan so fresh jobs are enqueued with the current SESSION_ID
      setTimeout(() => triggerImmediateScan().catch(console.error), 500);
    } catch (err: any) {
      console.error('❌ Watchdog failed to restart worker consumer:', err.message);
    } finally {
      workerHealth.workerRestarting = false;
    }
  } finally {
    watchdogRunning = false;
  }
}

async function initialize(): Promise<void> {
  console.log('🚀 Outreach Worker starting...');
  console.log(`   Session: ${SESSION_ID}`);
  console.log(`   Supabase: ${config.supabase.url}`);
  console.log(`   Redis: ${config.redis.url.replace(/\/\/.*@/, '//***@')}`);

  // Redis connection event logging
  connection.on('connect', () => console.log('✅ Redis connection established'));
  connection.on('close', () => console.error('❌ Redis connection closed'));
  connection.on('reconnecting', (delay: number) => console.warn(`⚠️ Redis reconnecting in ${delay}ms...`));
  connection.on('error', (err: Error) => console.error('❌ Redis error:', err.stack ?? err.message));

  // Verify Redis connection
  await connection.ping();
  console.log('✅ Redis connected');

  // ── Startup recovery ──────────────────────────────────────────────────
  // 1. Drain stale jobs from the previous session so the new SESSION_ID
  //    starts with a clean slate (old failed/stale jobs won't block enqueues).
  await drainExecutionQueue();
  await workerManager.drainAllAccountQueues(); // drain per-account queues too

  // 2. Reset all in_progress executions — they belong to the dead previous
  //    instance and will never complete without a reset.
  await resetInProgressOrphans();

  // 3. Sanity check: confirm priority_cohort backfill has been deployed.
  //    If count is 0, cohort-based prioritization is a no-op (no in_flight rows
  //    exist yet). This is normal before Lovable's backfill runs; the log line
  //    lets us confirm both sides are deployed and cohorts are populated.
  try {
    const { count: inFlightCount } = await supabase
      .from('unipile_sequence_executions')
      .select('id', { count: 'exact', head: true })
      .eq('status', 'running')
      .eq('priority_cohort', 'in_flight');
    const suffix = (inFlightCount ?? 0) === 0
      ? ' — backfill not yet deployed, cohort prioritization is a no-op'
      : '';
    console.log(`📊 Startup: priority_cohort=in_flight running executions: ${inFlightCount ?? 0}${suffix}`);
  } catch (e: any) {
    console.warn('⚠️ Startup: could not query in_flight cohort count:', e.message);
  }
  // ─────────────────────────────────────────────────────────────────────

  // Start workers
  startExecutionWorker();           // handles delay/conditional/other steps
  await workerManager.start();      // manages per-account LinkedIn & email workers
  startBatchWorker();
  await startScanner();

  // Watchdog: check every 15s, restart if stale for 60s with pending work
  setInterval(runWatchdog, 15_000);

  // Heartbeat: log worker health every 30s for post-mortem diagnosis
  setInterval(async () => {
    const lastCompleted = workerHealth.lastJobCompletedAt;
    const ageSeconds = lastCompleted
      ? Math.round((Date.now() - lastCompleted.getTime()) / 1000)
      : null;
    let pending = -1;
    try {
      const [w, a, d] = await Promise.all([
        executionQueue.getWaitingCount(),
        executionQueue.getActiveCount(),
        executionQueue.getDelayedCount(),
      ]);
      pending = w + a + d;
    } catch { /* ignore */ }
    const mem = process.memoryUsage();
    console.log(
      `[heartbeat] session=${SESSION_ID} worker=${!!workerHealth.worker} ` +
      `accountWorkers=${workerManager.getActiveWorkerCount()} ` +
      `partition=${config.partition}/${config.partitionCount} ` +
      `lastCompleted=${ageSeconds !== null ? `${ageSeconds}s ago` : 'never'} ` +
      `pending=${pending} redis=${connection.status} ` +
      `mem=${Math.round(mem.heapUsed / 1024 / 1024)}MB/${Math.round(mem.rss / 1024 / 1024)}MB`
    );
  }, 30_000);

  initialized = true;
  console.log('✅ Initialization complete');

  // Write initial heartbeat row so Lovable dashboard sees this replica immediately.
  await writeHeartbeat();
  // Heartbeat every 30s — Lovable marks a replica unhealthy after 2 min silence.
  setInterval(() => writeHeartbeat().catch(() => {}), 30_000);
}

async function main() {
  // Catch process-level crashes so we get a stack trace before Railway restarts us.
  process.on('uncaughtException', (err) => {
    console.error('💥 Uncaught exception — process will exit:', err.stack ?? err.message);
  });
  process.on('unhandledRejection', (reason) => {
    console.error('💥 Unhandled rejection:', reason instanceof Error ? reason.stack : reason);
  });

  // Graceful shutdown: write final heartbeat so Lovable dashboard shows offline state.
  process.on('SIGTERM', async () => {
    console.log('📴 SIGTERM received — writing final heartbeat and shutting down');
    await writeHeartbeat({ redis_connected: false, meta: { shutdown_reason: 'SIGTERM', session: SESSION_ID } }).catch(() => {});
    process.exit(0);
  });

  // ── HTTP server starts FIRST so Railway healthchecks pass immediately ──
  const app = express();

  app.get('/health', async (_req, res) => {
    // During startup, return 200 with ready:false so Railway doesn't kill us
    // before initialization completes.
    if (!initialized) {
      res.status(200).json({
        status: 'starting',
        ready: false,
        session: SESSION_ID,
        uptime: Math.round(process.uptime()),
      });
      return;
    }

    const now = Date.now();
    const lastCompleted = workerHealth.lastJobCompletedAt?.getTime() ?? 0;
    const workerStale = process.uptime() * 1000 > WATCHDOG_WARMUP_MS &&
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

    // Collect per-account queue stats (best-effort — skip on error)
    let accountQueues: Array<{ queueName: string; pending: number; active: number }> = [];
    try {
      accountQueues = await workerManager.getWorkerStats();
    } catch { /* non-fatal */ }

    res.status(healthy ? 200 : 503).json({
      status: healthy ? 'ok' : 'unhealthy',
      ready: true,
      session: SESSION_ID,
      uptime: Math.round(process.uptime()),
      partition: config.partition,
      partitionCount: config.partitionCount,
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
      activeAccountWorkers: workerManager.getActiveWorkerCount(),
      accountQueues,
      memory: {
        heapMB: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
        rssMB: Math.round(process.memoryUsage().rss / 1024 / 1024),
      },
    });
  });

  // Bull Board dashboard
  const bullBoardAdapter = setupBullBoard();
  app.use('/admin/queues', bullBoardAdapter.getRouter());

  // Start listening, then run heavy initialization in the background.
  await new Promise<void>((resolve) => {
    app.listen(config.port, () => {
      console.log(`✅ HTTP server on port ${config.port}`);
      console.log(`   Health: http://localhost:${config.port}/health`);
      console.log(`   Dashboard: http://localhost:${config.port}/admin/queues`);
      resolve();
    });
  });

  // Heavy init runs after the server is already accepting connections.
  initialize().catch((err) => {
    console.error('Fatal initialization error:', err.stack ?? err);
    process.exit(1);
  });
}

main().catch((err) => {
  console.error('Fatal error:', err.stack ?? err);
  process.exit(1);
});
