/**
 * outreach-orchestrator service entrypoint.
 *
 * Wiring only — no logic. The actual decision-making lives in scheduler.ts.
 * This file boots the subscribers and the HTTP health server.
 *
 * Started as a separate Railway service from the worker. Same repo, same
 * Dockerfile, different `start` script:
 *
 *     "start:orchestrator": "node dist/orchestrator/index.js"
 *
 * Build command stays `npm run build` (esbuild already bundles src/index.ts
 * and src/orchestrator/index.ts is bundled separately via an additional
 * esbuild target — see package.json).
 */

import express from 'express';
import { config } from '../config';
import { startRealtimeSubscriber, isRealtimeHealthy, subscriberStats } from './realtime-subscriber';
import { startPollFallback } from './poll-fallback';
import { handleWakeEvent } from './scheduler';
import { modeCacheStats } from './mode-reader';
import { clearAllInflightCounters } from './dispatch-budget';

const app = express();

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    service: 'outreach-orchestrator',
    realtimeHealthy: isRealtimeHealthy(),
    realtime: subscriberStats(),
    modeCache: modeCacheStats(),
    dryRun: config.dryRun,
  });
});

async function main() {
  console.log('🎼 Outreach Orchestrator starting...');
  console.log(`   Supabase: ${config.supabase.url}`);
  console.log(`   DRY_RUN: ${config.dryRun}`);

  // Wipe any stale `dispatch:inflight:*` counters from a previous
  // orchestrator session. If the prior process crashed between INCRing
  // a counter and successfully enqueueing the BullMQ job, those
  // reservations would never have been released and the next startup
  // would see a sequence apparently stuck at the cap forever (until
  // the 24h TTL). Clearing on boot keeps the cap aligned with reality;
  // genuine in-flight jobs that complete during the recovery window
  // call releaseDispatchSlot, which is a clamped DECR — safe.
  try {
    const cleared = await clearAllInflightCounters();
    if (cleared > 0) {
      console.log(`🧹 Cleared ${cleared} stale dispatch-budget counter(s)`);
    }
  } catch (err) {
    console.warn('Failed to clear dispatch-budget counters on startup:', err);
  }

  // Subscribers wired into the same handler. Realtime is primary; poll
  // fallback is recovery. Both call handleWakeEvent.
  startRealtimeSubscriber(handleWakeEvent);
  startPollFallback(handleWakeEvent);

  const port = config.port;
  app.listen(port, () => {
    console.log(`✅ Orchestrator HTTP on port ${port}`);
    console.log(`   Health: http://localhost:${port}/health`);
    console.log('✅ Orchestrator ready');
  });
}

main().catch((err) => {
  console.error('Orchestrator boot failed:', err);
  process.exit(1);
});
