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
