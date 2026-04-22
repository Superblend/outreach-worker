/**
 * One-time recovery script: drains stale delayed jobs from per-account BullMQ queues.
 *
 * Run once after deploying the queue-poisoning fix, then restart worker replicas.
 * Only touches delayed jobs — active and waiting jobs are left untouched.
 *
 * Usage:
 *   npx ts-node scripts/recover-poisoned-queues.ts
 *
 * Required env vars (same as worker):
 *   REDIS_URL, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
 */

import IORedis from 'ioredis';
import { Queue } from 'bullmq';
import { createClient } from '@supabase/supabase-js';

const REDIS_URL = process.env.REDIS_URL;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!REDIS_URL || !SUPABASE_URL || !SUPABASE_KEY) {
  console.error('❌ Missing required env vars: REDIS_URL, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const connection = new IORedis(REDIS_URL, {
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
});

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function scanKeys(pattern: string): Promise<string[]> {
  const results: string[] = [];
  let cursor = '0';
  do {
    const [nextCursor, batch] = await connection.scan(cursor, 'MATCH', pattern, 'COUNT', 200);
    results.push(...batch);
    cursor = nextCursor;
  } while (cursor !== '0');
  return results;
}

async function checkStale(
  executionId: string,
  stepId: string | undefined,
): Promise<{ stale: boolean; reason: string }> {
  const { data: row, error } = await supabase
    .from('unipile_sequence_executions')
    .select('status, current_step_id')
    .eq('id', executionId)
    .single();

  if (error || !row) {
    return { stale: true, reason: 'execution_not_found' };
  }
  if (row.status !== 'running') {
    return { stale: true, reason: `status=${row.status}` };
  }
  if (stepId && row.current_step_id !== stepId) {
    return { stale: true, reason: `step_advanced (job=${stepId} current=${row.current_step_id})` };
  }
  if (stepId) {
    const { data: result } = await (supabase
      .from('unipile_step_results')
      .select('id')
      .eq('execution_id', executionId)
      .eq('step_id', stepId)
      .eq('status', 'success') as any).maybeSingle();
    if (result) {
      return { stale: true, reason: 'already_succeeded' };
    }
  }
  return { stale: false, reason: '' };
}

async function main() {
  console.log('🔍 Scanning Redis for account queues...\n');

  const rawKeys: string[] = [];
  for (const prefix of ['bull:outreach-linkedin-', 'bull:outreach-email-client-']) {
    const batch = await scanKeys(`${prefix}*:id`);
    rawKeys.push(...batch);
  }

  const queueNames = rawKeys
    .map(k => k.match(/^bull:(outreach-(?:linkedin|email-client)-.+):id$/)?.[1])
    .filter((n): n is string => !!n);

  if (queueNames.length === 0) {
    console.log('No account queues found — nothing to recover.');
    await connection.quit();
    return;
  }

  console.log(`Found ${queueNames.length} account queue(s).\n`);

  let totalDelayed = 0;
  let totalRemoved = 0;
  let totalKept = 0;

  for (const queueName of queueNames) {
    const queue = new Queue(queueName, { connection });

    let delayed: Awaited<ReturnType<typeof queue.getDelayed>>;
    try {
      delayed = await queue.getDelayed();
    } catch (err: any) {
      console.warn(`  ⚠️  ${queueName}: could not fetch delayed jobs — ${err.message}`);
      await queue.close();
      continue;
    }

    if (delayed.length === 0) {
      await queue.close();
      continue;
    }

    let removed = 0;
    let kept = 0;

    for (const job of delayed) {
      const executionId: string | undefined = job.data?.execution_id;
      const stepId: string | undefined = job.data?.step_id;

      if (!executionId) {
        console.log(`  🗑️  [${queueName}] job=${job.id} reason=no_execution_id`);
        await job.remove();
        removed++;
        continue;
      }

      const { stale, reason } = await checkStale(executionId, stepId);
      if (stale) {
        console.log(`  🗑️  [${queueName}] job=${job.id} exec=${executionId} step=${stepId ?? 'unknown'} reason=${reason}`);
        await job.remove();
        removed++;
      } else {
        kept++;
      }
    }

    console.log(`  ${queueName}: ${delayed.length} delayed → removed=${removed} kept=${kept}`);
    totalDelayed += delayed.length;
    totalRemoved += removed;
    totalKept += kept;

    await queue.close();
  }

  console.log(`\n✅ Recovery complete.`);
  console.log(`   Total delayed checked : ${totalDelayed}`);
  console.log(`   Removed (stale)       : ${totalRemoved}`);
  console.log(`   Kept (still valid)    : ${totalKept}`);

  await connection.quit();
}

main().catch(err => {
  console.error('❌ Fatal error:', err);
  process.exit(1);
});
