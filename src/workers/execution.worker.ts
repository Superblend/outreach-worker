import { Worker, Job } from 'bullmq';
import { connection } from '../queues/definitions';
import { invokeEdgeFunction } from '../supabase';

interface ExecutionJobData {
  execution_id: string;
  group_key: string;
  channel: string;
}

export function startExecutionWorker() {
  const worker = new Worker<ExecutionJobData>(
    'outreach:executions',
    async (job: Job<ExecutionJobData>) => {
      const { execution_id, group_key, channel } = job.data;

      console.log(`🚀 Processing execution ${execution_id} (${group_key})`);

      const { data, error } = await invokeEdgeFunction(
        'unipile-execute-sequence-step',
        { execution_id }
      );

      if (error) {
        console.error(`❌ Execution ${execution_id} failed:`, error);
        throw new Error(`Edge function error: ${JSON.stringify(error)}`);
      }

      if (data?.skipped) {
        console.log(`⏭️ Execution ${execution_id} skipped: ${data.message || 'already processed'}`);
        return { skipped: true };
      }

      console.log(`✅ Execution ${execution_id} completed`);
      return { success: true, data };
    },
    {
      connection,
      concurrency: 10, // Max 10 parallel jobs across all accounts
      limiter: {
        max: 30,       // Max 30 jobs per 60 seconds globally
        duration: 60_000,
      },
    }
  );

  worker.on('failed', (job, err) => {
    console.error(`❌ Job ${job?.id} failed (attempt ${job?.attemptsMade}):`, err.message);
  });

  worker.on('completed', (job) => {
    console.log(`✅ Job ${job.id} completed`);
  });

  console.log('✅ Execution worker started');
  return worker;
}
