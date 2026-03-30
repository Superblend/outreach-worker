import { Worker, Job } from 'bullmq';
import { connection } from '../queues/definitions';
import { invokeEdgeFunction } from '../supabase';

interface BatchJobData {
  client_id: string;
}

export function startBatchWorker() {
  const worker = new Worker<BatchJobData>(
    'outreach-batches',
    async (job: Job<BatchJobData>) => {
      const { client_id } = job.data;

      console.log(`📦 Processing batches for client ${client_id}`);

      const { data, error } = await invokeEdgeFunction(
        'unipile-process-batch-queue',
        { client_id }
      );

      if (error) {
        console.error(`❌ Batch processing for client ${client_id} failed:`, error);
        throw new Error(`Edge function error: ${JSON.stringify(error)}`);
      }

      console.log(`✅ Batch processing for client ${client_id} completed`);
      return { success: true, data };
    },
    {
      connection,
      concurrency: 5,
    }
  );

  worker.on('failed', (job, err) => {
    console.error(`❌ Batch job ${job?.id} failed:`, err.message);
  });

  console.log('✅ Batch worker started');
  return worker;
}
