import { Worker, Job } from 'bullmq';
import { randomBytes } from 'crypto';
import { connection, executionQueue, batchQueue, scannerQueue, recoveryQueue, getAccountQueue } from './queues/definitions';
import { supabase } from './supabase';
import { config } from './config';
import { workerHealth } from './health';
import { localMinutesOfDay, localDateString, localWeekday } from './lib/time-utils';
import { workerManager } from './worker-manager';

// One random 4-byte hex per process lifetime.
// Stable within a session (prevents queue pile-up), unique across restarts
// (prevents dead failed/stale jobs from blocking new enqueues with the same execution+step pair).
export const SESSION_ID = randomBytes(4).toString('hex');

const LINKEDIN_STEP_TYPES = [
  'linkedin_invitation', 'linkedin_message', 'linkedin_profile_visit',
  'linkedin_voice_note', 'linkedin_engage_post', 'linkedin_endorse',
];

function convertToUTC(dateStr: string, hour: number, minute: number, timezone: string): Date {
  const timeStr = `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:00`;
  const isoString = `${dateStr}T${timeStr}.000Z`;
  const date = new Date(isoString);
  const utcDate = new Date(date.toLocaleString('en-US', { timeZone: 'UTC' }));
  const tzDate = new Date(date.toLocaleString('en-US', { timeZone: timezone }));
  const offsetMs = tzDate.getTime() - utcDate.getTime();
  return new Date(date.getTime() - offsetMs);
}

function cohortPriority(
  cohort: string | null | undefined,
  firstTouchDone: boolean,
  nextExecutionAt: string,
  now: number = Date.now()
): number {
  if (cohort === 'in_flight') {
    const overdueMs = now - new Date(nextExecutionAt).getTime();
    if (overdueMs > 86_400_000) return 1;  // overdue >24h: most urgent
    if (overdueMs > 3_600_000)  return 2;  // overdue 1–24h
    if (overdueMs > 900_000)    return 3;  // overdue 15m–1h
    return 6;                               // on-time: below new_today
  }
  if (cohort === 'new_today') return firstTouchDone ? 5 : 4;
  return 7; // low_priority / null
}

async function scanAndEnqueue() {
  workerHealth.lastScannerRunAt = new Date();
  const now = new Date();
  const nowIso = now.toISOString();
  const updatedAtBuffer = new Date(now.getTime() - 30000).toISOString();

  // ========================================
  // SCAN DUE EXECUTIONS (use_bullmq=true only)
  // Two-pass approach: overdue in_flight rows are fetched first so they
  // are always enqueued regardless of how many new-campaign executions exist.
  // The partial index idx_executions_overdue_inflight (deployed by Lovable)
  // makes Pass 1 a fast index scan.
  // ========================================
  const EXECUTION_SELECT = `
    id, batch_number, next_execution_at, updated_at,
    unipile_sequence_id, current_step_id,
    assigned_linkedin_account_id, assigned_email_account_id,
    priority_cohort, first_touch_done,
    unipile_sequences!inner(
      status, scheduled_start_time, scheduled_end_time,
      timezone, active_days, client_id, use_bullmq, worker_partition
    ),
    unipile_sequence_steps!unipile_sequence_executions_current_step_id_fkey(step_type)
  `;

  // Pass 1 — overdue in_flight (next_execution_at ≥ 15 min ago)
  const fifteenMinAgo = new Date(now.getTime() - 15 * 60_000).toISOString();
  const pass1Limit = Math.floor(config.scanLimit * 0.4); // up to 40% of budget
  const { data: overdueRows, error: overdueErr } = await supabase
    .from('unipile_sequence_executions')
    .select(EXECUTION_SELECT)
    .eq('status', 'running')
    .eq('execution_state', 'not_started')
    .eq('priority_cohort', 'in_flight')
    .eq('unipile_sequences.status', 'active')
    .eq('unipile_sequences.use_bullmq', true)
    .lte('next_execution_at', fifteenMinAgo)
    .lt('updated_at', updatedAtBuffer)
    .order('next_execution_at', { ascending: true })
    .limit(pass1Limit);

  if (overdueErr) console.error('Scanner: overdue pass failed:', overdueErr.message);

  // Pass 2 — all due now (remaining budget, deduped against Pass 1)
  const seenIds = new Set((overdueRows || []).map((e: any) => e.id));
  const pass2Limit = config.scanLimit - (overdueRows?.length || 0);
  const { data: generalRows, error: generalErr } = await supabase
    .from('unipile_sequence_executions')
    .select(EXECUTION_SELECT)
    .eq('status', 'running')
    .eq('execution_state', 'not_started')
    .eq('unipile_sequences.status', 'active')
    .eq('unipile_sequences.use_bullmq', true)
    .lte('next_execution_at', nowIso)
    .lt('updated_at', updatedAtBuffer)
    .order('batch_number', { ascending: true, nullsFirst: true })
    .order('next_execution_at', { ascending: true })
    .limit(pass2Limit);

  if (generalErr) console.error('Scanner: general pass failed:', generalErr.message);

  const dueExecutions = [
    ...(overdueRows || []),
    ...(generalRows || []).filter((e: any) => !seenIds.has(e.id)),
  ];

  console.log(
    `Scanner: found ${dueExecutions.length} due executions ` +
    `(overdue in_flight=${overdueRows?.length || 0}, general=${(generalRows || []).filter((e: any) => !seenIds.has(e.id)).length})`
  );

  if (dueExecutions && dueExecutions.length > 0) {
    // Filter by time window and active days, reschedule invalid ones
    const validExecs: typeof dueExecutions = [];
    
    for (const exec of dueExecutions) {
      const seq = exec.unipile_sequences as any;
      const timezone = seq?.timezone || 'UTC';
      const activeDays = seq?.active_days || [0,1,2,3,4,5,6];
      
      // localWeekday uses formatToParts — immune to en-US locale hour12 quirks
      const currentDayOfWeek = localWeekday(now, timezone);

      if (!activeDays.includes(currentDayOfWeek)) {
        // Reschedule to the start of the next active day (in local timezone)
        const [startHour, startMinute] = (seq?.scheduled_start_time || '09:00').split(':').map(Number);
        let daysUntilNext = 1;
        for (let i = 1; i <= 7; i++) {
          if (activeDays.includes((currentDayOfWeek + i) % 7)) { daysUntilNext = i; break; }
        }
        const nextDate = new Date(now.getTime() + daysUntilNext * 86_400_000);
        // Use local date (not UTC date) so the timezone offset doesn't shift us to the wrong day
        const nextRunUTC = convertToUTC(localDateString(nextDate, timezone), startHour, startMinute, timezone);

        await supabase.from('unipile_sequence_executions')
          .update({ next_execution_at: nextRunUTC.toISOString(), updated_at: new Date().toISOString() })
          .eq('id', exec.id);
        console.log(`⏸️ ${exec.id} inactive day (local weekday=${currentDayOfWeek}) → ${nextRunUTC.toISOString()}`);
        continue;
      }

      // Check time window using formatToParts — avoids the "2 AM" / NaN bug from
      // toLocaleString('en-US', { hour: 'numeric', hour12: false }) on some Node images
      if (seq?.scheduled_start_time && seq?.scheduled_end_time) {
        const [startH, startM] = seq.scheduled_start_time.split(':').map(Number);
        const [endH, endM] = seq.scheduled_end_time.split(':').map(Number);
        const curMin = localMinutesOfDay(now, timezone);
        const startMin = startH * 60 + startM;
        const endMin = endH * 60 + endM;

        console.log(`Scanner: [${exec.id}] timezone=${timezone} localMin=${curMin} window=${startMin}-${endMin}`);

        if (curMin < startMin || curMin > endMin) {
          // Use local date (not UTC) when computing the next window start
          const targetDate = curMin > endMin
            ? new Date(now.getTime() + 86_400_000)  // past end → tomorrow local
            : now;                                    // before start → today local
          const nextRunUTC = convertToUTC(localDateString(targetDate, timezone), startH, startM, timezone);
          await supabase.from('unipile_sequence_executions')
            .update({ next_execution_at: nextRunUTC.toISOString(), updated_at: new Date().toISOString() })
            .eq('id', exec.id);
          console.log(`⏸️ ${exec.id} outside window (localMin=${curMin}, window=${startMin}-${endMin}) → ${nextRunUTC.toISOString()}`);
          continue;
        }
      }
      
      validExecs.push(exec);
    }

    // Client partitioning: each worker replica handles a subset of clients
    const PARTITION = config.partition;
    const PARTITION_COUNT = config.partitionCount;
    const partitioned = PARTITION_COUNT <= 1
      ? validExecs
      : validExecs.filter(exec => {
          const wp: number = (exec.unipile_sequences as any)?.worker_partition ?? 0;
          return (wp % PARTITION_COUNT) === PARTITION;
        });

    // Group by account+channel and enqueue with appropriate delays
    const groups = new Map<string, Array<typeof partitioned[0]>>();

    console.log(
      `Scanner: ${validExecs.length} passed time/day filters → ` +
      `${partitioned.length} assigned to partition ${PARTITION}/${PARTITION_COUNT} ` +
      `(of ${dueExecutions.length} found)`
    );

    for (const exec of partitioned) {
      // Normalize: PostgREST may return steps as an object or a single-element array
      const rawStepData = exec.unipile_sequence_steps as any;
      const stepData = Array.isArray(rawStepData) ? rawStepData[0] : rawStepData;
      const stepType = stepData?.step_type || 'unknown';
      let key: string;

      if (LINKEDIN_STEP_TYPES.includes(stepType)) {
        key = `linkedin:${exec.assigned_linkedin_account_id || 'unknown'}`;
      } else if (stepType === 'email') {
        // Email workers are per-client, not per-account
        const clientId = (exec.unipile_sequences as any)?.client_id || 'unknown';
        key = `email-client:${clientId}`;
      } else {
        key = `other:${exec.id}`;
      }

      console.log(`Scanner: grouping exec=${exec.id} stepType=${stepType} key=${key}`);

      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(exec);
    }

    // Enqueue jobs into per-account queues (LinkedIn) or per-client queues (email)
    // or shared queue (other). No scanner-side delays — each worker enforces its own
    // pacing (45-90s LinkedIn per account / 2-3s email per client) via the Lua slot script.
    for (const [groupKey, execs] of groups) {
      const colonIdx = groupKey.indexOf(':');
      const channel = groupKey.slice(0, colonIdx);
      const entityId = groupKey.slice(colonIdx + 1);
      let enqueued = 0;

      // Determine target queue:
      //   linkedin       → per-account queue (WorkerManager creates the worker)
      //   email-client   → per-client queue  (WorkerManager creates the worker)
      //   other          → shared outreach-executions queue
      let targetQueueName: string;
      if (channel === 'linkedin' && entityId && entityId !== 'unknown') {
        targetQueueName = `outreach-linkedin-${entityId}`;
      } else if (channel === 'email-client' && entityId && entityId !== 'unknown') {
        targetQueueName = `outreach-email-client-${entityId}`;
      } else {
        targetQueueName = 'outreach-executions';
      }
      const targetQueue = targetQueueName === 'outreach-executions'
        ? executionQueue
        : getAccountQueue(targetQueueName);

      for (let i = 0; i < execs.length; i++) {
        const exec = execs[i];

        // Re-derive stepType here — the grouping loop's stepType is out of scope.
        const rawStep = exec.unipile_sequence_steps as any;
        const stepData = Array.isArray(rawStep) ? rawStep[0] : rawStep;
        const execStepType = stepData?.step_type || 'unknown';
        const execStepId: string = exec.current_step_id;

        // Session-scoped stable jobId:
        //   - Stable within a session → BullMQ deduplicates waiting/delayed jobs,
        //     preventing queue pile-up when the worker is backlogged.
        //   - Session suffix rotates on every process restart → old failed/stale jobs
        //     from the previous session can't block new enqueues (the permanent deadlock fix).
        const jobId = `exec-${exec.id}-${execStepId}-${SESSION_ID}`;
        console.log(`Scanner: enqueueing exec=${exec.id} queue=${targetQueueName} jobId=${jobId}`);

        // Normalise channel for job data: 'email-client' → 'email' so the worker
        // processor can still branch on channel === 'email'.
        const jobChannel = channel === 'email-client' ? 'email' : channel;
        const priority = targetQueueName !== 'outreach-executions'
          ? cohortPriority(
              (exec as any).priority_cohort,
              (exec as any).first_touch_done === true,
              exec.next_execution_at,
            )
          : undefined;
        try {
          await targetQueue.add(
            'execute-step',
            {
              execution_id: exec.id,
              group_key: groupKey,
              channel: jobChannel,
              step_id: execStepId,
              step_type: execStepType,
            },
            {
              attempts: 3,
              backoff: { type: 'exponential', delay: 5000 },
              jobId,
              ...(priority !== undefined && { priority }),
              removeOnComplete: { age: 3600, count: 1000 },
              removeOnFail: { age: 86400, count: 5000 },
            }
          );
          enqueued++;
        } catch (enqueueErr: any) {
          console.error(`Scanner: ❌ Failed to enqueue exec=${exec.id}:`, enqueueErr.stack ?? enqueueErr.message);
        }
      }

      // Spawn a worker for this account queue immediately if one doesn't exist yet.
      // This is the primary mechanism — don't wait for the 30s reconcile cycle.
      if (targetQueueName !== 'outreach-executions' && enqueued > 0) {
        workerManager.ensureWorker(targetQueueName);
      }

      console.log(`📦 Enqueued ${enqueued}/${execs.length} jobs for ${groupKey} → ${targetQueueName}`);
    }

    // Mark all enqueued executions as touched so the scanner skips them for
    // the next 30s instead of re-enqueuing the same stable jobId every cycle.
    // BullMQ deduplicates stable jobIds anyway, but this keeps the logs clean.
    if (partitioned.length > 0) {
      const touchedAt = new Date().toISOString();
      const ids = partitioned.map(e => e.id);
      for (let i = 0; i < ids.length; i += 100) {
        await supabase
          .from('unipile_sequence_executions')
          .update({ updated_at: touchedAt })
          .in('id', ids.slice(i, i + 100));
      }
    }
  }

  // ========================================
  // SCAN DUE BATCHES (use_bullmq=true only)
  // ========================================
  const { data: dueBatches, error: batchError } = await supabase
    .from('unipile_batch_queue')
    .select(`
      id, unipile_sequence_id,
      unipile_sequences!inner(client_id, use_bullmq)
    `)
    .eq('status', 'pending')
    .eq('unipile_sequences.use_bullmq', true)
    .lte('scheduled_for', nowIso);

  if (batchError) {
    console.error('Scanner: failed to fetch batches:', batchError);
    return;
  }

  if (dueBatches && dueBatches.length > 0) {
    // Group by client_id
    const clientIds = [...new Set(
      dueBatches.map((b: any) => b.unipile_sequences?.client_id).filter(Boolean)
    )];

    for (const clientId of clientIds) {
      await batchQueue.add(
        'process-batch',
        { client_id: clientId },
        {
          attempts: 3,
          backoff: { type: 'exponential', delay: 5000 },
          jobId: `batch:${clientId}:${Date.now()}`,
          removeOnComplete: { age: 3600, count: 500 },
          removeOnFail: { age: 86400, count: 1000 },
        }
      );
    }
    
    console.log(`📦 Enqueued batch jobs for ${clientIds.length} clients`);
  }
}

async function recoveryPass() {
  const now = new Date();
  const nowIso = now.toISOString();

  // Release orphaned in_progress claims (worker crashed mid-job > 10 min ago)
  const orphanCutoff = new Date(now.getTime() - 10 * 60_000).toISOString();
  const { data: orphans, error: orphanErr } = await supabase
    .from('unipile_sequence_executions')
    .select('id')
    .eq('execution_state', 'in_progress')
    .eq('status', 'running')
    .lt('updated_at', orphanCutoff);

  if (!orphanErr && orphans && orphans.length > 0) {
    const staleAt = new Date(now.getTime() - 31_000).toISOString();
    const { error: releaseErr } = await supabase
      .from('unipile_sequence_executions')
      .update({ execution_state: 'not_started', updated_at: staleAt })
      .in('id', orphans.map((r: any) => r.id));
    if (releaseErr) {
      console.error(`❌ Recovery: failed to release orphaned claims:`, releaseErr.message);
    } else {
      console.log(`🔧 Recovery: released ${orphans.length} orphaned in_progress claims`);
    }
  }

  const recoveryWindowEnd = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000).toISOString();

  const { data: stranded, error } = await supabase
    .from('unipile_sequence_executions')
    .select(`
      id, next_execution_at, execution_log, current_step_id,
      unipile_sequence_steps!unipile_sequence_executions_current_step_id_fkey(
        step_type, delay_value, delay_unit, configuration
      ),
      unipile_sequences!inner(client_id, use_bullmq)
    `)
    .eq('status', 'running')
    .eq('unipile_sequences.use_bullmq', true)
    .gt('next_execution_at', nowIso)
    .lte('next_execution_at', recoveryWindowEnd)
    .limit(100);

  if (error || !stranded?.length) return;

  let recovered = 0;
  for (const exec of stranded) {
    const step = exec.unipile_sequence_steps as any;
    if (!step || step.step_type !== 'delay') continue;

    let delayValue = step.delay_value || step.configuration?.delay_value || 1;
    let delayUnit = step.delay_unit || step.configuration?.delay_unit || 'days';
    
    let delayMs = 0;
    switch (delayUnit) {
      case 'minutes': delayMs = delayValue * 60_000; break;
      case 'hours': delayMs = delayValue * 3_600_000; break;
      default: delayMs = delayValue * 86_400_000;
    }

    const log = Array.isArray(exec.execution_log) ? exec.execution_log : [];
    const lastEntry = log[log.length - 1];
    const anchorTime = lastEntry?.executed_at ? new Date(lastEntry.executed_at).getTime() : null;
    if (!anchorTime) continue;

    if (anchorTime + delayMs <= now.getTime()) {
      await supabase.from('unipile_sequence_executions')
        .update({ 
          next_execution_at: nowIso,
          updated_at: new Date(now.getTime() - 31000).toISOString()
        })
        .eq('id', exec.id);
      recovered++;
      console.log(`🔧 Recovered stranded delay: ${exec.id}`);
    }
  }

  if (recovered > 0) console.log(`🔧 Recovery pass: fixed ${recovered} stranded executions`);
}

export async function startScanner() {
  // Set up repeatable scanner job (every 60s)
  await scannerQueue.upsertJobScheduler(
    'scan-due-executions',
    { every: config.scanIntervalMs },
    { name: 'scan', data: {} }
  );

  // Set up repeatable recovery job (every 5min)
  await recoveryQueue.upsertJobScheduler(
    'recovery-pass',
    { every: config.recoveryIntervalMs },
    { name: 'recover', data: {} }
  );

  // Scanner worker
  new Worker('outreach-scanner', async (_job: Job) => {
    console.log('🔍 Scanner cycle starting...');
    await scanAndEnqueue();
    console.log('🔍 Scanner cycle complete');
  }, { connection, concurrency: 1 });

  // Recovery worker
  new Worker('outreach-recovery', async (_job: Job) => {
    console.log('🔧 Recovery pass starting...');
    await recoveryPass();
  }, { connection, concurrency: 1 });

  console.log(`✅ Scanner started (session=${SESSION_ID}, interval: ${config.scanIntervalMs / 1000}s, recovery: 5min, limit: ${config.scanLimit})`);
}

/** Run a scanner cycle immediately (used by watchdog after worker restart). */
export async function triggerImmediateScan(): Promise<void> {
  console.log('🔍 Watchdog-triggered scan starting...');
  await scanAndEnqueue();
  console.log('🔍 Watchdog-triggered scan complete');
}
