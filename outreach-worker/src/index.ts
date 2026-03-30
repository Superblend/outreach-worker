import express from 'express';
import { config } from './config';
import { startScanner } from './scanner';
import { startExecutionWorker } from './workers/execution.worker';
import { startBatchWorker } from './workers/batch.worker';
import { setupBullBoard } from './monitoring/bull-board';
import { connection } from './queues/definitions';

async function main() {
  console.log('🚀 Outreach Worker starting...');
  console.log(`   Supabase: ${config.supabase.url}`);
  console.log(`   Redis: ${config.redis.url.replace(/\/\/.*@/, '//***@')}`);

  // Verify Redis connection
  await connection.ping();
  console.log('✅ Redis connected');

  // Start workers
  startExecutionWorker();
  startBatchWorker();
  await startScanner();

  // HTTP server for health checks + Bull Board
  const app = express();

  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', uptime: process.uptime() });
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
