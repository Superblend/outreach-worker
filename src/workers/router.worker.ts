/**
 * Router worker — second tier of the orchestrator dispatch pipeline.
 *
 * Consumes minimal "route-dispatch" jobs that the orchestrator pushes to
 * `outreach-dispatch-pending`, resolves the execution's step_type and
 * assigned accounts from Supabase, and re-enqueues to the appropriate
 * destination queue:
 *
 *   - LinkedIn step → `outreach-linkedin-{accountId}`  (per-account)
 *   - Email step    → `outreach-email-client-{clientId}` (per-client)
 *   - Other step    → `outreach-executions`              (shared)
 *
 * Why this is its own worker and not inline in the orchestrator:
 *   - Doc's contract: orchestrator decides WHEN, worker owns queue topology.
 *   - Lets us keep account-rotation logic (sticky-per-lead) in worker code
 *     where the existing scanner.ts pattern already lives.
 *   - Pure event-driven — no DB polling here; consumes BullMQ jobs that the
 *     orchestrator's Realtime-driven scheduler produced.
 *
 * High concurrency, no external calls (just one DB read + one BullMQ add).
 * Stale-job protection: re-checks the execution's status before re-enqueueing
 * so we don't push to per-account queues for executions that completed,
 * paused, or already advanced past this step in the meantime.
 */

import { Worker, Job } from 'bullmq';
import { randomBytes } from 'crypto';
import {
  connection,
  executionQueue,
  getAccountQueue,
} from '../queues/definitions';
import { supabase } from '../supabase';

/** Per-process session ID for jobId stability at the destination queue. */
const ROUTER_SESSION_ID = randomBytes(4).toString('hex');

const LINKEDIN_STEP_TYPES = new Set([
  'linkedin_invitation',
  'linkedin_message',
  'linkedin_profile_visit',
  'linkedin_voice_note',
  'linkedin_engage_post',
  'linkedin_endorse',
]);

interface RouterJobPayload {
  execution_id: string;
  step_id: string;
  client_id: string;
  contact_id: string;
  cohort_priority: number;
  cohort_label: string;
}

interface ExecutionRow {
  id: string;
  status: string;
  execution_state: string;
  current_step_id: string | null;
  assigned_linkedin_account_id: string | null;
  assigned_email_account_id: string | null;
  unipile_sequence_steps:
    | { step_type: string }
    | { step_type: string }[]
    | null;
}

export function startRouterWorker(): Worker {
  const worker = new Worker(
    'outreach-dispatch-pending',
    async (job: Job<RouterJobPayload>) => {
      const { execution_id, step_id, client_id, contact_id, cohort_priority } =
        job.data;

      // 1. Re-fetch the execution + its current step (skip if no longer eligible).
      const { data, error } = await supabase
        .from('unipile_sequence_executions')
        .select(`
          id, status, execution_state, current_step_id,
          assigned_linkedin_account_id, assigned_email_account_id,
          unipile_sequence_steps!unipile_sequence_executions_current_step_id_fkey(step_type)
        `)
        .eq('id', execution_id)
        .maybeSingle();

      if (error) {
        console.error(
          `[router] fetch failed exec=${execution_id}: ${error.message}`,
        );
        throw new Error(`fetch failed: ${error.message}`);
      }
      if (!data) {
        console.warn(`[router] execution missing — skipping exec=${execution_id}`);
        return;
      }

      const exec = data as unknown as ExecutionRow;

      // Idempotency: if the execution advanced past the step the orchestrator
      // decided about, skip. The worker will pick up the new step on its
      // next wake. This also catches the case where scanner.ts and
      // orchestrator both produced decisions for the same step.
      if (exec.status !== 'running') {
        console.log(
          `[router] skipping exec=${execution_id} status=${exec.status} step=${step_id}`,
        );
        return;
      }
      if (exec.current_step_id !== step_id) {
        console.log(
          `[router] step advanced — skipping exec=${execution_id} ` +
            `decided=${step_id} now=${exec.current_step_id}`,
        );
        return;
      }

      // 2. Resolve step type.
      const rawStep = exec.unipile_sequence_steps;
      const stepData = Array.isArray(rawStep) ? rawStep[0] : rawStep;
      const stepType = stepData?.step_type ?? 'unknown';

      // 3. Decide destination queue.
      let targetQueueName: string;
      let channel: string;
      if (LINKEDIN_STEP_TYPES.has(stepType)) {
        const accountId = exec.assigned_linkedin_account_id;
        if (accountId) {
          targetQueueName = `outreach-linkedin-${accountId}`;
          channel = 'linkedin';
        } else {
          // No LinkedIn account assigned yet — fall through to shared queue.
          // The execution.worker.ts main consumer can handle assignment
          // (the existing rotation logic) or skip if no account is available.
          targetQueueName = 'outreach-executions';
          channel = 'linkedin';
        }
      } else if (stepType === 'email') {
        targetQueueName = `outreach-email-client-${client_id}`;
        channel = 'email';
      } else {
        targetQueueName = 'outreach-executions';
        channel = stepType;
      }

      const targetQueue =
        targetQueueName === 'outreach-executions'
          ? executionQueue
          : getAccountQueue(targetQueueName);

      // Fresh per-dispatch nonce so BullMQ jobId dedup doesn't silently drop
      // legitimate re-routes for an execution that was previously refused by
      // the worker's daily-cap RPC. Worker's stale-job check on
      // `unipile_step_results` is the actual duplicate-send protection.
      const dispatchNonce = randomBytes(3).toString('hex');
      const destJobId = `exec-${execution_id}-${step_id}-${ROUTER_SESSION_ID}-${dispatchNonce}`;
      const groupKey =
        channel === 'linkedin'
          ? `linkedin:${exec.assigned_linkedin_account_id || 'unknown'}`
          : channel === 'email'
            ? `email-client:${client_id}`
            : `other:${execution_id}`;

      // 4. Re-enqueue to the destination queue. Same payload shape that
      //    execution.worker.ts already expects (matches scanner.ts output).
      await targetQueue.add(
        'execute-step',
        {
          execution_id,
          group_key: groupKey,
          channel,
          step_id,
          step_type: stepType,
        },
        {
          attempts: 3,
          backoff: { type: 'exponential', delay: 5000 },
          jobId: destJobId,
          priority: cohort_priority,
          removeOnComplete: { age: 3600, count: 1000 },
          removeOnFail: { age: 86400, count: 5000 },
        },
      );

      console.log(
        `[router] routed exec=${execution_id} step_type=${stepType} ` +
          `→ ${targetQueueName} priority=${cohort_priority}`,
      );
    },
    {
      connection,
      // Routing is cheap (one DB read, one Redis add). Default concurrency
      // suits a few hundred routes/second easily.
      concurrency: 30,
    },
  );

  worker.on('active', (job) => {
    console.log(`[router] ▶ job=${job.id} processing`);
  });
  worker.on('completed', (job) => {
    console.log(`[router] ✓ job=${job.id} completed`);
  });
  worker.on('failed', (job, err) => {
    console.error(
      `[router] ✗ job=${job?.id} failed (attempt ${job?.attemptsMade}/${job?.opts?.attempts}): ${err.message}`,
    );
  });

  console.log(
    `[router] ✅ Router worker started (queue=outreach-dispatch-pending, concurrency=30, session=${ROUTER_SESSION_ID})`,
  );
  return worker;
}
