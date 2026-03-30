import { Worker, Job } from 'bullmq';
import { connection, executionQueue, batchQueue, scannerQueue, recoveryQueue } from './queues/definitions';
import { supabase } from './supabase';
import { config } from './config';

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

async function scanAndEnqueue() {
  const now = new Date();
  const nowIso = now.toISOString();
  const updatedAtBuffer = new Date(now.getTime() - 30000).toISOString();

  // ========================================
  // SCAN DUE EXECUTIONS (use_bullmq=true only)
  // ========================================
  const { data: dueExecutions, error: execError } = await supabase
    .from('unipile_sequence_executions')
    .select(`
      id, batch_number, next_execution_at, updated_at,
      unipile_sequence_id, current_step_id,
      assigned_linkedin_account_id, assigned_email_account_id,
      unipile_sequences!inner(
        status, scheduled_start_time, scheduled_end_time,
        timezone, active_days, client_id, use_bullmq
      ),
      unipile_sequence_steps!unipile_sequence_executions_current_step_id_fkey(step_type)
    `)
    .eq('status', 'running')
    .eq('unipile_sequences.status', 'active')
    .eq('unipile_sequences.use_bullmq', true)
    .lte('next_execution_at', nowIso)
    .lt('updated_at', updatedAtBuffer)
    .order('batch_number', { ascending: true, nullsFirst: true })
    .order('next_execution_at', { ascending: true })
    .limit(500);

  if (execError) {
    console.error('Scanner: failed to fetch executions:', execError);
    return;
  }

  console.log(`Scanner: found ${dueExecutions?.length || 0} due executions`);

  if (dueExecutions && dueExecutions.length > 0) {
    // Filter by time window and active days, reschedule invalid ones
    const validExecs: typeof dueExecutions = [];
    
    for (const exec of dueExecutions) {
      const seq = exec.unipile_sequences as any;
      const timezone = seq?.timezone || 'UTC';
      const activeDays = seq?.active_days || [0,1,2,3,4,5,6];
      
      const localDayStr = now.toLocaleString('en-US', { timeZone: timezone, weekday: 'short' });
      const dayMap: Record<string, number> = { 'Sun': 0, 'Mon': 1, 'Tue': 2, 'Wed': 3, 'Thu': 4, 'Fri': 5, 'Sat': 6 };
      const currentDayOfWeek = dayMap[localDayStr] ?? now.getUTCDay();
      
      if (!activeDays.includes(currentDayOfWeek)) {
        // Reschedule to next active day
        const [startHour, startMinute] = (seq?.scheduled_start_time || '09:00').split(':').map(Number);
        let daysUntilNext = 1;
        for (let i = 1; i <= 7; i++) {
          if (activeDays.includes((currentDayOfWeek + i) % 7)) { daysUntilNext = i; break; }
        }
        const nextDate = new Date(now);
        nextDate.setDate(nextDate.getDate() + daysUntilNext);
        const nextRunUTC = convertToUTC(nextDate.toISOString().split('T')[0], startHour, startMinute, timezone);
        
        await supabase.from('unipile_sequence_executions')
          .update({ next_execution_at: nextRunUTC.toISOString(), updated_at: new Date().toISOString() })
          .eq('id', exec.id);
        console.log(`⏸️ ${exec.id} on inactive day → ${nextRunUTC.toISOString()}`);
        continue;
      }
      
      // Check time window
      if (seq?.scheduled_start_time && seq?.scheduled_end_time) {
        const [startH, startM] = seq.scheduled_start_time.split(':').map(Number);
        const [endH, endM] = seq.scheduled_end_time.split(':').map(Number);
        const localTimeStr = now.toLocaleString('en-US', { timeZone: timezone, hour: 'numeric', minute: 'numeric', hour12: false });
        const [lh, lm] = localTimeStr.split(':').map((s: string) => parseInt(s, 10));
        const curMin = lh * 60 + lm;
        const startMin = startH * 60 + startM;
        const endMin = endH * 60 + endM;
        
        if (curMin < startMin || curMin > endMin) {
          const nextRunDate = new Date(now);
          if (curMin > endMin) nextRunDate.setDate(nextRunDate.getDate() + 1);
          const nextRunUTC = convertToUTC(nextRunDate.toISOString().split('T')[0], startH, startM, timezone);
          await supabase.from('unipile_sequence_executions')
            .update({ next_execution_at: nextRunUTC.toISOString(), updated_at: new Date().toISOString() })
            .eq('id', exec.id);
          console.log(`⏸️ ${exec.id} outside time window → ${nextRunUTC.toISOString()}`);
          continue;
        }
      }
      
      validExecs.push(exec);
    }

    // Group by account+channel and enqueue with appropriate delays
    const groups = new Map<string, Array<typeof validExecs[0]>>();
    
    for (const exec of validExecs) {
      const stepData = exec.unipile_sequence_steps as any;
      const stepType = stepData?.step_type || 'unknown';
      let key: string;
      
      if (LINKEDIN_STEP_TYPES.includes(stepType)) {
        key = `linkedin:${exec.assigned_linkedin_account_id || 'unknown'}`;
      } else if (stepType === 'email') {
        key = `email:${exec.assigned_email_account_id || 'unknown'}`;
      } else {
        key = `other:${exec.id}`;
      }
      
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(exec);
    }

    // Enqueue jobs with staggered delays per account group
    for (const [groupKey, execs] of groups) {
      const [channel] = groupKey.split(':');
      
      for (let i = 0; i < execs.length; i++) {
        const exec = execs[i];
        let delay = 0;
        
        if (channel === 'linkedin') {
          // Stagger within the group: i-th job delayed by i * (8-15s)
          delay = i * (config.linkedinInterSendDelayMs + Math.random() * config.linkedinJitterMs);
        } else if (channel === 'email') {
          // Small stagger for emails
          const batchIndex = Math.floor(i / config.emailBatchSize);
          delay = batchIndex * (config.emailInterSendDelayMs + Math.random() * config.emailJitterMs);
        }
        
        await executionQueue.add(
          'execute-step',
          {
            execution_id: exec.id,
            group_key: groupKey,
            channel,
          },
          {
            delay: Math.round(delay),
            attempts: 3,
            backoff: { type: 'exponential', delay: 5000 },
            jobId: `exec:${exec.id}:${Date.now()}`, // Unique per scan cycle
            removeOnComplete: { age: 3600, count: 1000 },
            removeOnFail: { age: 86400, count: 5000 },
          }
        );
      }
      
      console.log(`📦 Enqueued ${execs.length} jobs for ${groupKey}`);
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
  new Worker('outreach:scanner', async (_job: Job) => {
    console.log('🔍 Scanner cycle starting...');
    await scanAndEnqueue();
    console.log('🔍 Scanner cycle complete');
  }, { connection, concurrency: 1 });

  // Recovery worker
  new Worker('outreach:recovery', async (_job: Job) => {
    console.log('🔧 Recovery pass starting...');
    await recoveryPass();
  }, { connection, concurrency: 1 });

  // Also run one scan immediately on start
  await scanAndEnqueue();

  console.log('✅ Scanner started (interval: 60s, recovery: 5min)');
}
