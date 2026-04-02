import { Worker, Job } from 'bullmq';
import { connection, executionQueue } from '../queues/definitions';
import { supabase, invokeEdgeFunction } from '../supabase';
import { config } from '../config';
import { sendEmail } from '../lib/unipile-send-email';
import { sendLinkedInInvitation } from '../lib/unipile-send-linkedin-invitation';
import { sendLinkedInMessage } from '../lib/unipile-send-linkedin-message';
import { visitProfile } from '../lib/unipile-visit-profile';
import { checkConnection } from '../lib/unipile-check-connection';
import { unipileFetch } from '../lib/unipile-fetch';
import { engagePost } from '../lib/unipile-engage-post';
import { BatchWriter } from '../lib/batch-db';

interface ExecutionJobData {
  execution_id: string;
  group_key: string;
  channel: string;
}

const LINKEDIN_STEP_TYPES = [
  'linkedin_invitation', 'linkedin_message', 'linkedin_profile_visit',
  'linkedin_voice_note', 'linkedin_engage_post', 'linkedin_endorse',
];

const SENDING_STEP_TYPES = ['linkedin_invitation', 'linkedin_message', 'email'];
const GATING_STEP_TYPES = ['linkedin_invitation'];

const LINKEDIN_PACING_MIN_MS = 45_000;
const LINKEDIN_PACING_MAX_MS = 90_000;
const LINKEDIN_PACING_TTL_S = 300; // safety TTL: 5 min

// Atomically checks and acquires the per-account LinkedIn pacing slot.
// Returns 0 if the slot is acquired (caller may proceed), or the number of ms
// to wait if the slot is still locked by a recent send.
// Uses a Lua script so the read-compare-set is atomic against Redis.
const PACING_LUA = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local minGap = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local lastSend = redis.call('GET', key)
if lastSend then
  local elapsed = now - tonumber(lastSend)
  if elapsed < minGap then
    return minGap - elapsed
  end
end
redis.call('SET', key, tostring(now), 'EX', ttl)
return 0
`;

async function acquireLinkedInSlot(accountId: string): Promise<number> {
  const key = `linkedin:${accountId}`;
  const now = Date.now();
  const gap = Math.round(LINKEDIN_PACING_MIN_MS + Math.random() * (LINKEDIN_PACING_MAX_MS - LINKEDIN_PACING_MIN_MS));
  const waitMs = await connection.eval(PACING_LUA, 1, key, String(now), String(gap), String(LINKEDIN_PACING_TTL_S)) as number;
  return waitMs;
}

const EMAIL_PACING_MIN_MS = 10_000;
const EMAIL_PACING_MAX_MS = 20_000;
const EMAIL_PACING_TTL_S = 120;

const EMAIL_PACING_LUA = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local gap = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local last = tonumber(redis.call('GET', key) or '0')
local elapsed = now - last
if elapsed >= gap then
  redis.call('SET', key, tostring(now), 'EX', ttl)
  return 0
else
  return gap - elapsed
end
`;

async function acquireEmailSlot(accountId: string): Promise<number> {
  const key = `email:${accountId}`;
  const now = Date.now();
  const gap = Math.round(EMAIL_PACING_MIN_MS + Math.random() * (EMAIL_PACING_MAX_MS - EMAIL_PACING_MIN_MS));
  const waitMs = await connection.eval(EMAIL_PACING_LUA, 1, key, String(now), String(gap), String(EMAIL_PACING_TTL_S)) as number;
  return waitMs;
}

function calculateDelay(value: number, unit: string): number {
  switch (unit) {
    case 'minutes': return value * 60 * 1000;
    case 'hours': return value * 60 * 60 * 1000;
    case 'days': return value * 24 * 60 * 60 * 1000;
    default: return value * 24 * 60 * 60 * 1000;
  }
}

function convertToUTC(dateStr: string, hour: number, minute: number, timezone: string): Date {
  const timeStr = `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:00`;
  const isoString = `${dateStr}T${timeStr}.000Z`;
  const date = new Date(isoString);
  const utcDate = new Date(date.toLocaleString('en-US', { timeZone: 'UTC' }));
  const tzDate = new Date(date.toLocaleString('en-US', { timeZone: timezone }));
  const offsetMs = tzDate.getTime() - utcDate.getTime();
  return new Date(date.getTime() - offsetMs);
}

function isRetryableError(message: string): boolean {
  const lower = message.toLowerCase();
  return (
    lower.includes('rate limit') ||
    lower.includes('429') ||
    lower.includes('503') ||
    lower.includes('timeout') ||
    lower.includes('network') ||
    lower.includes('500') ||
    lower.includes('502') ||
    lower.includes('edge function')
  );
}

function isNonRetryableError(message: string): boolean {
  const lower = message.toLowerCase();
  return (
    lower.includes('already connected') ||
    lower.includes('profile not found') ||
    lower.includes('unauthorized') ||
    lower.includes('invalid') ||
    lower.includes('missing required') ||
    lower.includes('follow_up_threading_rejected')
  );
}

const CONNECTION_STATUS_ERROR_PATTERNS = [
  'not connected', 'not first degree', 'cannot create chat', 'chat create failed',
  'already connected', 'connection exists', 'pending invitation', 'already_invited',
  'already_invited_recently', 'recipient cannot be reached', 'profile not found',
  'cannot message', 'cannot invite',
];

const LINKEDIN_CONNECTION_STEP_TYPES = ['linkedin_invitation', 'linkedin_message', 'linkedin_engage_post'];

function isConnectionStatusError(message: string): boolean {
  const lower = message.toLowerCase();
  return CONNECTION_STATUS_ERROR_PATTERNS.some(pattern => lower.includes(pattern));
}

function getConnectionStatusReason(message: string): string {
  const lower = message.toLowerCase();
  if (lower.includes('already connected') || lower.includes('connection exists')) return 'already_connected';
  if (lower.includes('pending invitation') || lower.includes('already_invited') || lower.includes('already_invited_recently')) return 'invitation_pending';
  if (lower.includes('profile not found')) return 'profile_not_found';
  return 'not_connected';
}

async function getNextStepId(currentStepId: string, handle?: string): Promise<string | null> {
  const query = supabase
    .from('unipile_sequence_edges')
    .select('target_step_id')
    .eq('source_step_id', currentStepId);

  if (handle) {
    (query as any).eq('source_handle', handle);
  }

  const { data } = await (query as any).maybeSingle();
  return data?.target_step_id || null;
}

async function enforceTimeWindow(proposedTime: Date, sequence: any): Promise<Date> {
  if (!sequence?.scheduled_start_time || !sequence?.scheduled_end_time) {
    return proposedTime;
  }

  const timezone = sequence.timezone || 'UTC';
  const [startH, startM] = sequence.scheduled_start_time.split(':').map(Number);
  const [endH, endM] = sequence.scheduled_end_time.split(':').map(Number);

  const localTimeStr = proposedTime.toLocaleString('en-US', {
    timeZone: timezone, hour: 'numeric', minute: 'numeric', hour12: false,
  });
  const [lh, lm] = localTimeStr.split(':').map((s: string) => parseInt(s, 10));
  const curMin = lh * 60 + lm;
  const startMin = startH * 60 + startM;
  const endMin = endH * 60 + endM;

  if (curMin >= startMin && curMin <= endMin) return proposedTime;

  const nextRunDate = new Date(proposedTime);
  if (curMin > endMin) nextRunDate.setDate(nextRunDate.getDate() + 1);
  const dateStr = nextRunDate.toLocaleDateString('en-CA', { timeZone: timezone });
  return convertToUTC(dateStr, startH, startM, timezone);
}

async function smartReschedule(
  execution: any,
  accountId: string,
  messageType: string,
  defaultLimit: number,
): Promise<string> {
  const sequence = execution.unipile_sequences as any;
  const timezone = sequence?.timezone || 'UTC';
  const activeDays = sequence?.active_days || [0, 1, 2, 3, 4, 5, 6];
  const [startH, startM] = (sequence?.scheduled_start_time || '09:00').split(':').map(Number);
  const [endH, endM] = (sequence?.scheduled_end_time || '17:00').split(':').map(Number);
  const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

  const now = new Date();
  for (let dayOffset = 0; dayOffset <= 14; dayOffset++) {
    const candidate = new Date(now.getTime() + dayOffset * 86_400_000);
    const localDayStr = candidate.toLocaleString('en-US', { timeZone: timezone, weekday: 'short' });
    const localDay = dayNames.indexOf(localDayStr.substring(0, 3));
    if (localDay === -1 || !activeDays.includes(localDay)) continue;

    const { data: limitCheck } = await supabase.rpc('check_and_increment_daily_limit', {
      p_account_id: accountId,
      p_message_type: messageType,
      p_max_default: defaultLimit,
      p_dry_run: true,
    });

    if (limitCheck && !limitCheck.allowed) continue;

    const windowMs = (endH * 60 + endM - startH * 60 - startM) * 60 * 1000;
    const jitter = Math.random() * Math.min(windowMs, 3_600_000);
    const dateStr = candidate.toLocaleDateString('en-CA', { timeZone: timezone });
    const baseTime = convertToUTC(dateStr, startH, startM, timezone);
    return new Date(baseTime.getTime() + jitter).toISOString();
  }

  // Fallback: tomorrow at start time
  const tomorrow = new Date(now.getTime() + 86_400_000);
  const tomorrowStr = tomorrow.toLocaleDateString('en-CA', { timeZone: timezone });
  return convertToUTC(tomorrowStr, startH, startM, timezone).toISOString();
}

async function checkAndRerouteFromConditional(
  executionLog: any[],
  steps: any[],
): Promise<string | null> {
  const lastEntry = executionLog[executionLog.length - 1];
  if (!lastEntry || lastEntry.step_type !== 'conditional' || lastEntry.action !== 'condition_no') {
    return null;
  }

  const conditionalStep = steps.find((s: any) => s.id === lastEntry.step_id);
  if (!conditionalStep) return null;

  const { data: yesEdge } = await supabase
    .from('unipile_sequence_edges')
    .select('target_step_id')
    .eq('source_step_id', conditionalStep.id)
    .eq('source_handle', 'yes')
    .maybeSingle();

  return yesEdge?.target_step_id || null;
}

async function executeStep(execution_id: string, stepResultWriter: BatchWriter, job: Job<ExecutionJobData>) {
  // 1. Fetch execution data
  const { data: execution, error: execError } = await supabase
    .from('unipile_sequence_executions')
    .select(`
      *, sequence_version,
      unipile_sequences(*, scheduled_start_time, scheduled_end_time, timezone, active_days, client_id, status),
      unipile_sequence_steps(*)
    `)
    .eq('id', execution_id)
    .single();

  if (execError || !execution) {
    throw new Error(`Failed to fetch execution ${execution_id}: ${execError?.message}`);
  }

  // Resolve entity (lead or contact) by explicit query
  const leadId = (execution as any).lead_id;
  const contactId = (execution as any).contact_id;
  console.log(`🔍 [${execution_id}] Resolving entity: lead_id=${leadId}, contact_id=${contactId}`);

  let lead: any;
  if (leadId) {
    const { data: leadData, error: leadErr } = await supabase
      .from('leads')
      .select('first_name, last_name, email, linkedin, company, position, industry')
      .eq('id', leadId)
      .single();
    if (leadErr) {
      console.error(`❌ [${execution_id}] Failed to fetch lead ${leadId}:`, leadErr.message);
    }
    lead = leadData;
  } else if (contactId) {
    const { data: contactData, error: contactErr } = await supabase
      .from('contacts')
      .select('first_name, last_name, email, linkedin, company, position, industry')
      .eq('id', contactId)
      .single();
    if (contactErr) {
      console.error(`❌ [${execution_id}] Failed to fetch contact ${contactId}:`, contactErr.message);
    }
    lead = contactData;
  } else {
    const msg = `Execution has neither lead_id nor contact_id`;
    console.error(`❌ [${execution_id}] ${msg}`);
    await supabase.from('unipile_sequence_executions')
      .update({ status: 'failed', error_message: msg, updated_at: new Date().toISOString() })
      .eq('id', execution_id);
    return;
  }

  if (!lead) {
    const msg = `Entity not found (lead_id=${leadId}, contact_id=${contactId})`;
    console.error(`❌ [${execution_id}] ${msg}`);
    await supabase.from('unipile_sequence_executions')
      .update({ status: 'failed', error_message: msg, updated_at: new Date().toISOString() })
      .eq('id', execution_id);
    return;
  }
  console.log(`✅ [${execution_id}] Resolved entity: ${lead.first_name} ${lead.last_name} <${lead.email}> linkedin=${lead.linkedin}`);
  const sequence = (execution as any).unipile_sequences as any;
  const executionLog: any[] = Array.isArray(execution.execution_log) ? execution.execution_log as any[] : [];

  // 4. Pre-checks
  if (execution.status !== 'running') {
    console.log(`⏭️ Execution ${execution_id} not running (${execution.status}), skipping`);
    return;
  }

  if (sequence?.status === 'paused') {
    await supabase.from('unipile_sequence_executions')
      .update({ status: 'paused', updated_at: new Date().toISOString() })
      .eq('id', execution_id);
    console.log(`⏸️ Execution ${execution_id} paused (sequence paused)`);
    return; // No claim acquired, no release needed
  }

  // Claim execution for processing (prevents duplicate sends across concurrent workers)
  const { data: claimed, error: claimError } = await supabase.rpc('claim_execution_for_processing', { p_execution_id: execution_id });
  console.log(`🔒 [${execution_id}] Claim result: claimed=${claimed}${claimError ? ` claimError=${claimError.message}` : ''}`);
  if (!claimed) {
    if (claimError) {
      console.error(`❌ [${execution_id}] Claim RPC failed:`, claimError.message);
    } else {
      console.log(`⏭️ [${execution_id}] Already claimed by another worker, skipping`);
    }
    return;
  }

  try {

  // 3. Load all steps
  const { data: allSteps } = await supabase
    .from('unipile_sequence_steps')
    .select('*')
    .eq('unipile_sequence_id', execution.unipile_sequence_id)
    .order('step_order');

  const steps = (allSteps || []) as any[];
  const firstStep = steps[0];

  // 2. Account resolution
  let assignedEmailAccountId: string = (execution as any).assigned_email_account_id;
  let assignedLinkedInAccountId: string = (execution as any).assigned_linkedin_account_id;

  if (assignedEmailAccountId) {
    const { data: emailAccount } = await supabase
      .from('unipile_accounts')
      .select('id, status, email, client_id, provider')
      .eq('id', assignedEmailAccountId)
      .single();

    if (emailAccount?.status === 'disconnected') {
      const { data: replacement } = await (supabase
        .from('unipile_accounts')
        .select('id')
        .eq('status', 'active')
        .eq('email', emailAccount.email)
        .eq('client_id', emailAccount.client_id)
        .eq('provider', emailAccount.provider)
        .limit(1) as any).maybeSingle();

      if (replacement) {
        assignedEmailAccountId = replacement.id;
        await supabase.from('unipile_sequence_executions')
          .update({ assigned_email_account_id: replacement.id })
          .eq('id', execution_id);
      }
    }
  }

  console.log(`🏦 [${execution_id}] Accounts: linkedin=${assignedLinkedInAccountId || 'none'}, email=${assignedEmailAccountId || 'none'}`);

  // 5. First-step connection check
  const isFirstStep = executionLog.length === 0;

  if (isFirstStep && firstStep && (firstStep.step_type === 'linkedin_invitation' || firstStep.step_type === 'conditional')) {
    const linkedInAccountId = assignedLinkedInAccountId ||
      firstStep.configuration?.account_id ||
      steps.find((s: any) => LINKEDIN_STEP_TYPES.includes(s.step_type))?.configuration?.account_id;

    if (linkedInAccountId && lead) {
      const connResult = await checkConnection({ account_id: linkedInAccountId, lead });

      if (connResult.connected) {
        if (firstStep.step_type === 'conditional') {
          const { data: yesEdge } = await supabase
            .from('unipile_sequence_edges')
            .select('target_step_id')
            .eq('source_step_id', firstStep.id)
            .eq('source_handle', 'yes')
            .maybeSingle();

          if (yesEdge?.target_step_id) {
            await supabase.from('unipile_sequence_executions')
              .update({
                current_step_id: yesEdge.target_step_id,
                next_execution_at: new Date().toISOString(),
                execution_log: [...executionLog, {
                  step_id: firstStep.id,
                  step_type: 'conditional',
                  action: 'condition_yes',
                  executed_at: new Date().toISOString(),
                  result: { connected: true, chat_id: connResult.chat_id },
                }],
                chat_id: connResult.chat_id || (execution as any).chat_id,
                updated_at: new Date().toISOString(),
              })
              .eq('id', execution_id);
          } else {
            await supabase.from('unipile_sequence_executions')
              .update({ status: 'completed', updated_at: new Date().toISOString() })
              .eq('id', execution_id);
          }
        } else {
          // linkedin_invitation: already connected, complete
          await supabase.from('unipile_sequence_executions')
            .update({
              status: 'completed',
              execution_log: [...executionLog, {
                step_id: firstStep.id,
                step_type: 'linkedin_invitation',
                action: 'already_connected_completed',
                executed_at: new Date().toISOString(),
              }],
              updated_at: new Date().toISOString(),
            })
            .eq('id', execution_id);
        }
        return;
      }
    }
  }

  // 6. Get current step
  const currentStepId = execution.current_step_id;
  const currentStep = steps.find((s: any) => s.id === currentStepId) || firstStep;

  if (!currentStep) {
    const msg = `No step found (current_step_id=${currentStepId}, total_steps=${steps.length})`;
    console.error(`❌ [${execution_id}] ${msg}`);
    await supabase.from('unipile_sequence_executions')
      .update({
        error_message: msg,
        next_execution_at: new Date(Date.now() + 300_000).toISOString(),
        updated_at: new Date().toISOString(),
      })
      .eq('id', execution_id);
    return;
  }

  console.log(`🔄 [${execution_id}] Step: ${currentStep.step_type} (${currentStep.id})`);

  // Auto-resolve missing LinkedIn account for LinkedIn steps
  if (!assignedLinkedInAccountId && LINKEDIN_STEP_TYPES.includes(currentStep.step_type)) {
    const { data: linkedInAccount } = await (supabase
      .from('unipile_accounts')
      .select('id')
      .eq('client_id', sequence?.client_id)
      .eq('provider', 'LINKEDIN')
      .eq('status', 'active')
      .limit(1) as any).maybeSingle();

    if (linkedInAccount) {
      assignedLinkedInAccountId = linkedInAccount.id;
      await supabase.from('unipile_sequence_executions')
        .update({ assigned_linkedin_account_id: linkedInAccount.id })
        .eq('id', execution_id);
      console.log(`🔧 [${execution_id}] Auto-resolved LinkedIn account: ${linkedInAccount.id}`);
    } else {
      console.warn(`⚠️ [${execution_id}] No active LinkedIn account found for client ${sequence?.client_id}`);
    }
  }

  // Log the resolved account for send steps
  if (SENDING_STEP_TYPES.includes(currentStep.step_type)) {
    const sendAccountId = currentStep.step_type === 'email'
      ? (assignedEmailAccountId || currentStep.configuration?.account_id)
      : (assignedLinkedInAccountId || currentStep.configuration?.account_id);

    if (sendAccountId) {
      const { data: acctRow, error: acctErr } = await supabase
        .from('unipile_accounts')
        .select('id, account_id, status, provider')
        .eq('id', sendAccountId)
        .single();
      if (acctErr) {
        console.error(`❌ [${execution_id}] Account lookup failed for ${sendAccountId}:`, acctErr.message);
      } else {
        console.log(`🏦 [${execution_id}] Send account: internal=${sendAccountId} external=${acctRow?.account_id} status=${acctRow?.status} provider=${acctRow?.provider}`);
      }
    } else {
      console.warn(`⚠️ [${execution_id}] No account ID for ${currentStep.step_type} step — will fail`);
    }
  }

  // 7. Duplicate execution check
  if (SENDING_STEP_TYPES.includes(currentStep.step_type)) {
    const { data: existingResult } = await (supabase
      .from('unipile_step_results')
      .select('id')
      .eq('execution_id', execution_id)
      .eq('step_id', currentStep.id)
      .eq('status', 'success') as any).maybeSingle();

    if (existingResult) {
      console.log(`⏭️ Duplicate: step ${currentStep.id} already succeeded, advancing`);
      const nextStepId = await getNextStepId(currentStep.id);
      if (nextStepId) {
        const nextExecAt = await enforceTimeWindow(new Date(), sequence);
        await supabase.from('unipile_sequence_executions')
          .update({
            current_step_id: nextStepId,
            next_execution_at: nextExecAt.toISOString(),
            updated_at: new Date().toISOString(),
          })
          .eq('id', execution_id);
      } else {
        await supabase.from('unipile_sequence_executions')
          .update({ status: 'completed', updated_at: new Date().toISOString() })
          .eq('id', execution_id);
      }
      return;
    }
  }

  // 8. Cooldown guard (LinkedIn only)
  if (currentStep.step_type === 'linkedin_message' || currentStep.step_type === 'linkedin_invitation') {
    const twoMinAgo = new Date(Date.now() - 120_000).toISOString();
    const { data: recentSend } = await (supabase
      .from('unipile_step_results')
      .select('id')
      .eq('execution_id', execution_id)
      .eq('step_type', currentStep.step_type)
      .eq('status', 'success')
      .gt('created_at', twoMinAgo) as any).maybeSingle();

    if (recentSend) {
      const deferUntil = new Date(Date.now() + 120_000).toISOString();
      await supabase.from('unipile_sequence_executions')
        .update({ next_execution_at: deferUntil, updated_at: new Date().toISOString() })
        .eq('id', execution_id);
      console.log(`⏳ Cooldown guard for ${execution_id}, deferred 120s`);
      return;
    }
  }

  // 9. Daily limit check
  let preIncrementedAccountId: string | null = null;
  let preIncrementedMessageType: string | null = null;

  if (SENDING_STEP_TYPES.includes(currentStep.step_type)) {
    const messageType = currentStep.step_type === 'email' ? 'emails'
      : currentStep.step_type === 'linkedin_invitation' ? 'linkedin_invitations' : 'linkedin_messages';

    const accountId = currentStep.step_type === 'email'
      ? (assignedEmailAccountId || currentStep.configuration?.account_id)
      : (assignedLinkedInAccountId || currentStep.configuration?.account_id);

    const defaultLimit = messageType === 'linkedin_invitations' ? 30 : 50;

    if (accountId) {
      const { data: limitResult } = await supabase.rpc('check_and_increment_daily_limit', {
        p_account_id: accountId,
        p_message_type: messageType,
        p_max_default: currentStep.configuration?.daily_limit || defaultLimit,
      });
      console.log(`📊 Daily limit check result for ${execution_id} (${messageType}): ${JSON.stringify(limitResult)}`);

      if (limitResult && !limitResult.allowed) {
        const nextTime = await smartReschedule(execution, accountId, messageType, defaultLimit);
        await supabase.from('unipile_sequence_executions')
          .update({ next_execution_at: nextTime, updated_at: new Date().toISOString() })
          .eq('id', execution_id);
        console.log(`📅 Daily limit reached for ${execution_id}, rescheduled to ${nextTime}`);
        return;
      }

      if (limitResult?.allowed) {
        preIncrementedAccountId = accountId;
        preIncrementedMessageType = messageType;
      }
    }
  }

  // 10. Pre-step verification (LinkedIn message only)
  if (currentStep.step_type === 'linkedin_message') {
    const hasPriorInvitation = steps.some((s: any) =>
      s.step_type === 'linkedin_invitation' &&
      executionLog.some((l: any) => l.step_id === s.id && l.action === 'completed')
    );

    if (hasPriorInvitation) {
      const { data: acceptedEvent } = await (supabase
        .from('unipile_message_events')
        .select('id')
        .eq('execution_id', execution_id)
        .eq('event_type', 'invitation_accepted') as any).maybeSingle();

      if (!acceptedEvent) {
        await invokeEdgeFunction('verify-linkedin-connection', { execution_id });
      }
    }
  }

  // 11. Execute step
  let stepResult: any = null;
  let stepError: string | null = null;
  let nextStepId: string | null = null;
  let executedAt: string | null = null; // captured right after API responds

  try {
    switch (currentStep.step_type) {
      case 'linkedin_invitation': {
        const cfg = currentStep.configuration || {};
        const accountId = assignedLinkedInAccountId || cfg.account_id;
        if (!accountId) throw new Error('Missing required LinkedIn account');

        const result = await sendLinkedInInvitation({
          account_id: accountId,
          lead,
          message: cfg.message_body || cfg.message || '',
        });

        // Always capture result so webhook fields are persisted even on failure
        stepResult = result;
        if (result.success) executedAt = new Date().toISOString();

        if (result.success) {
          nextStepId = await getNextStepId(currentStep.id);
        } else {
          stepError = result.error || 'Unknown error';
          if (result.error?.toLowerCase().includes('already connected')) {
            const reroute = await checkAndRerouteFromConditional(executionLog, steps);
            if (reroute) {
              nextStepId = reroute;
              stepError = null;
            } else if (isFirstStep) {
              await supabase.from('unipile_sequence_executions')
                .update({ status: 'completed', updated_at: new Date().toISOString() })
                .eq('id', execution_id);
              if (preIncrementedAccountId) {
                try {
                  await supabase.rpc('rollback_daily_limit_increment', {
                    p_account_id: preIncrementedAccountId,
                    p_action_type: preIncrementedMessageType,
                    p_date: new Date().toISOString().split('T')[0],
                  });
                } catch (e: any) {
                  console.warn(`⚠️ Failed to rollback daily limit for ${preIncrementedAccountId}:`, e.message);
                }
              }
              return;
            }
          }
        }
        break;
      }

      case 'linkedin_message': {
        const cfg = currentStep.configuration || {};
        const accountId = assignedLinkedInAccountId || cfg.account_id;
        if (!accountId) throw new Error('Missing required LinkedIn account');

        const result = await sendLinkedInMessage({
          account_id: accountId,
          lead,
          message: cfg.message_body || cfg.message || '',
          use_inmail: cfg.use_inmail || false,
          chat_id: (execution as any).chat_id || cfg.chat_id,
        });

        if (result.success) {
          stepResult = result;
          executedAt = new Date().toISOString();
          nextStepId = await getNextStepId(currentStep.id);
        } else {
          stepError = result.error || 'Unknown error';
          if (result.error?.toLowerCase().includes('already connected') || result.error?.toLowerCase().includes('not connected')) {
            const reroute = await checkAndRerouteFromConditional(executionLog, steps);
            if (reroute) { nextStepId = reroute; stepError = null; }
          }
        }
        break;
      }

      case 'email': {
        const cfg = currentStep.configuration || {};
        const accountId = assignedEmailAccountId || cfg.account_id;
        if (!accountId) throw new Error('Missing required email account');

        let inReplyToMessageId: string | undefined;
        let originalSubject: string | undefined;

        if (cfg.is_follow_up) {
          const { data: firstEmailResult } = await (supabase
            .from('unipile_step_results')
            .select('response_data')
            .eq('execution_id', execution_id)
            .eq('status', 'success')
            .eq('step_type', 'email')
            .not('response_data->provider_id', 'is', null)
            .order('executed_at', { ascending: true })
            .limit(1) as any).maybeSingle();

          if (firstEmailResult?.response_data) {
            inReplyToMessageId = firstEmailResult.response_data.provider_id;
            originalSubject = firstEmailResult.response_data.subject;
          }
          // If no previous email found, send as new (not a reply)
          console.log(`📧 [${execution_id}] step=${currentStep.id} is_follow_up=true previous_found=${!!firstEmailResult} in_reply_to=${inReplyToMessageId || 'none'} original_subject="${originalSubject || 'none'}" final_subject="${cfg.subject || ''}"`);
        }

        const result = await sendEmail({
          account_id: accountId,
          lead,
          subject: cfg.subject || '',
          body: cfg.body || '',
          use_html: cfg.use_html || false,
          in_reply_to_message_id: inReplyToMessageId,
          original_subject: originalSubject,
        });

        if (result.success) {
          stepResult = result;
          executedAt = new Date().toISOString();
          nextStepId = await getNextStepId(currentStep.id);
        } else {
          stepError = result.error || 'Unknown error';
          // 422 on a follow-up means threading data was rejected — pause so data can be corrected
          if (cfg.is_follow_up && (stepError.includes('422') || stepError.includes('Unprocessable'))) {
            stepError = `follow_up_threading_rejected: ${stepError}`;
          }
        }
        break;
      }

      case 'delay': {
        const cfg = currentStep.configuration || {};
        const delayValue = cfg.delay_value || currentStep.delay_value || 1;
        const delayUnit = cfg.delay_unit || currentStep.delay_unit || 'days';
        const delayMs = calculateDelay(delayValue, delayUnit);

        // Verify delay was actually served (use execution_log anchor, NOT updated_at)
        const lastEntry = executionLog[executionLog.length - 1];
        const anchorTime = lastEntry?.executed_at ? new Date(lastEntry.executed_at).getTime() : null;

        if (anchorTime) {
          const elapsed = Date.now() - anchorTime;
          if (elapsed < delayMs * 0.9) {
            const remaining = delayMs - elapsed;
            const nextExecAt = new Date(Date.now() + remaining).toISOString();
            await supabase.from('unipile_sequence_executions')
              .update({ next_execution_at: nextExecAt, updated_at: new Date().toISOString() })
              .eq('id', execution_id);
            console.log(`⏳ Delay not served for ${execution_id}, rescheduling in ${Math.round(remaining / 1000)}s`);
            return;
          }
        }

        const delayLogEntry = {
          step_id: currentStep.id,
          step_type: 'delay',
          action: 'completed',
          executed_at: new Date().toISOString(),
        };

        const delayNextStepId = await getNextStepId(currentStep.id);

        if (delayNextStepId) {
          const nextStep = steps.find((s: any) => s.id === delayNextStepId);
          let nextExecAt: Date = new Date();
          if (nextStep?.step_type === 'delay') {
            const nv = nextStep.configuration?.delay_value || nextStep.delay_value || 1;
            const nu = nextStep.configuration?.delay_unit || nextStep.delay_unit || 'days';
            const rawDate = new Date(Date.now() + calculateDelay(nv, nu));
            if (nu === 'days' && sequence?.scheduled_start_time) {
              const tz = sequence.timezone || 'UTC';
              const [sH, sM] = sequence.scheduled_start_time.split(':').map(Number);
              const targetDayStr = rawDate.toLocaleDateString('en-CA', { timeZone: tz }); // YYYY-MM-DD
              nextExecAt = convertToUTC(targetDayStr, sH, sM, tz);
            } else {
              nextExecAt = rawDate;
            }
          }
          nextExecAt = await enforceTimeWindow(nextExecAt, sequence);

          await supabase.from('unipile_sequence_executions')
            .update({
              current_step_id: delayNextStepId,
              next_execution_at: nextExecAt.toISOString(),
              execution_log: [...executionLog, delayLogEntry],
              updated_at: new Date().toISOString(),
            })
            .eq('id', execution_id);
        } else {
          await supabase.from('unipile_sequence_executions')
            .update({
              status: 'completed',
              execution_log: [...executionLog, delayLogEntry],
              updated_at: new Date().toISOString(),
            })
            .eq('id', execution_id);
        }
        return;
      }

      case 'conditional': {
        const cfg = currentStep.configuration || {};
        const conditionType = cfg.condition_type || 'already_connected';
        const accountId = assignedLinkedInAccountId || cfg.account_id;

        let conditionMet = false;
        let chatId: string | undefined;

        if (conditionType === 'already_connected' && accountId && lead) {
          const connResult = await checkConnection({ account_id: accountId, lead });
          conditionMet = connResult.connected;
          chatId = connResult.chat_id;
        } else if (conditionType === 'check_connection' && accountId) {
          let isContact = false;
          let relationError: string | undefined;
          let checkProviderId: string | undefined;

          try {
            // Resolve the contact's LinkedIn URL
            const linkedInUrl: string | null =
              lead?.linkedin || lead?.linkedin_url || null;

            const profileIdMatch = linkedInUrl?.match(/linkedin\.com\/in\/([^/?#]+)/i);
            const linkedinProfileId = profileIdMatch ? profileIdMatch[1] : null;

            if (!linkedinProfileId) {
              relationError = 'No LinkedIn URL found for contact';
            } else {
              const { data: accountRow } = await supabase
                .from('unipile_accounts')
                .select('provider_account_id, account_id, status')
                .eq('id', accountId)
                .single();

              if (!accountRow || accountRow.status !== 'active') {
                relationError = `Account not found or not active: ${accountId}`;
              } else {
                const apiUrl = config.unipile.apiUrl;
                const unipileAccountId = accountRow.account_id;

                // Check connection via Relation API
                const res = await unipileFetch(
                  `${apiUrl}/api/v1/users/${accountRow.provider_account_id}/relation/${linkedinProfileId}`,
                  { headers: { 'X-API-KEY': config.unipile.apiKey } }
                );
                const data = await res.json().catch(() => null);

                if (!res.ok) {
                  relationError = data?.error || `Relation API failed: ${res.status}`;
                } else {
                  isContact = data?.is_contact === true;
                }

                // When connected, resolve provider_id and find existing chat_id
                if (isContact && unipileAccountId) {
                  try {
                    const profileRes = await unipileFetch(
                      `${apiUrl}/api/v1/users/${linkedinProfileId}?account_id=${unipileAccountId}`,
                      { headers: { 'X-API-KEY': config.unipile.apiKey } }
                    );
                    const profileData = await profileRes.json().catch(() => null);
                    checkProviderId = profileData?.id || profileData?.provider_id;

                    if (checkProviderId) {
                      // Search chats: max 100, 5 pages of 20
                      const pageSize = 20;
                      let page = 1;
                      let fetched = 0;

                      while (fetched < 100) {
                        const chatsUrl = `${apiUrl}/api/v1/chats?account_id=${unipileAccountId}&provider=LINKEDIN&limit=${pageSize}&page=${page}`;
                        const chatsRes = await unipileFetch(chatsUrl, { headers: { 'X-API-KEY': config.unipile.apiKey } });
                        const chatsData = await chatsRes.json().catch(() => null);
                        const chats: any[] = chatsData?.items || chatsData?.chats || [];

                        const match = chats.find((c: any) => c.attendee_provider_id === checkProviderId);
                        if (match) {
                          chatId = match.id;
                          break;
                        }

                        fetched += chats.length;
                        if (chats.length < pageSize) break;
                        page++;
                      }
                    }
                  } catch (chatErr: any) {
                    console.warn(`[check_connection] Chat lookup failed (non-critical):`, chatErr.message);
                  }
                }
              }
            }
          } catch (err: any) {
            relationError = err.message || 'Unexpected error during check_connection';
            console.error(`❌ check_connection error for ${execution_id}:`, err);
          }

          conditionMet = isContact;

          await stepResultWriter.add({
            execution_id,
            step_id: currentStep.id,
            lead_id: (execution as any).lead_id,
            contact_id: (execution as any).contact_id,
            step_type: 'conditional',
            status: 'success',
            error_message: null,
            response_data: {
              condition_result: isContact ? 'yes' : 'no',
              is_contact: isContact,
              provider_id: checkProviderId || null,
              chat_id: chatId || null,
              ...(relationError ? { error: relationError } : {}),
            },
          });
        }

        const handle = conditionMet ? 'yes' : 'no';
        const { data: condEdge } = await supabase
          .from('unipile_sequence_edges')
          .select('target_step_id')
          .eq('source_step_id', currentStep.id)
          .eq('source_handle', handle)
          .maybeSingle();

        const condLogEntry = {
          step_id: currentStep.id,
          step_type: 'conditional',
          action: `condition_${handle}`,
          executed_at: new Date().toISOString(),
          result: { conditionMet, chat_id: chatId },
        };

        if (condEdge?.target_step_id) {
          const nextExecAt = await enforceTimeWindow(new Date(), sequence);
          await supabase.from('unipile_sequence_executions')
            .update({
              current_step_id: condEdge.target_step_id,
              next_execution_at: nextExecAt.toISOString(),
              execution_log: [...executionLog, condLogEntry],
              chat_id: chatId || (execution as any).chat_id,
              updated_at: new Date().toISOString(),
            })
            .eq('id', execution_id);
        } else {
          await supabase.from('unipile_sequence_executions')
            .update({
              status: 'completed',
              execution_log: [...executionLog, condLogEntry],
              updated_at: new Date().toISOString(),
            })
            .eq('id', execution_id);
        }
        return;
      }

      case 'linkedin_profile_visit': {
        const cfg = currentStep.configuration || {};
        const accountId = assignedLinkedInAccountId || cfg.account_id;
        if (!accountId) throw new Error('Missing required LinkedIn account');

        const result = await visitProfile({ account_id: accountId, lead });
        stepResult = result;
        if (!result.success) stepError = result.error || 'Profile visit failed';
        nextStepId = await getNextStepId(currentStep.id);
        break;
      }

      case 'linkedin_voice_note': {
        const cfg = currentStep.configuration || {};
        const accountId = assignedLinkedInAccountId || cfg.account_id;
        if (!accountId) throw new Error('Missing required LinkedIn account');

        const result = await sendLinkedInMessage({
          account_id: accountId,
          lead,
          message: cfg.voice_note_text || cfg.message_body || '',
          use_inmail: false,
          chat_id: (execution as any).chat_id,
        });
        stepResult = result;
        if (!result.success) stepError = result.error || 'Voice note failed';
        nextStepId = await getNextStepId(currentStep.id);
        break;
      }

      case 'linkedin_engage_post': {
        const cfg = currentStep.configuration || {};
        const accountId = assignedLinkedInAccountId || cfg.account_id;
        if (!accountId) throw new Error('Missing required LinkedIn account');

        const result = await engagePost({
          account_id: accountId,
          lead,
          action_type: cfg.action_type || 'like',
          comment_text: cfg.comment_text,
        });

        stepResult = result;
        if (!result.success) stepError = result.error || 'Engage post failed';
        nextStepId = await getNextStepId(currentStep.id);
        break;
      }

      case 'linkedin_endorse': {
        const { data: efResult, error: efError } = await invokeEdgeFunction('unipile-endorse-skills', {
          execution_id,
          step_id: currentStep.id,
        });
        stepResult = efResult;
        if (efError) stepError = efError.message || 'Endorse failed';
        nextStepId = await getNextStepId(currentStep.id);
        break;
      }

      default:
        console.warn(`Unknown step type: ${currentStep.step_type}`);
        nextStepId = await getNextStepId(currentStep.id);
        break;
    }
  } catch (err: any) {
    stepError = err.message || 'Unexpected error during step execution';
    console.error(`❌ Step execution error for ${execution_id}:`, err);
  }

  // 11b. Connection-status error short-circuit for LinkedIn steps
  if (stepError && LINKEDIN_CONNECTION_STEP_TYPES.includes(currentStep.step_type) && isConnectionStatusError(stepError)) {
    const completedReason = getConnectionStatusReason(stepError);
    console.log(`🔌 Connection status error for ${execution_id} (${completedReason}): ${stepError}`);

    if (preIncrementedAccountId) {
      try {
        await supabase.rpc('rollback_daily_limit_increment', {
          p_account_id: preIncrementedAccountId,
          p_action_type: preIncrementedMessageType,
          p_date: new Date().toISOString().split('T')[0],
        });
      } catch (e: any) {
        console.warn(`⚠️ Failed to rollback daily limit for ${preIncrementedAccountId}:`, e.message);
      }
    }

    await stepResultWriter.add({
      execution_id,
      step_id: currentStep.id,
      lead_id: (execution as any).lead_id,
      contact_id: (execution as any).contact_id,
      step_type: currentStep.step_type,
      status: 'completed',
      response_data: { completed_reason: completedReason, connection_status_error: true, original_error: stepError },
      error_message: null,
      unipile_message_id: null,
      unipile_chat_id: null,
    });
    await stepResultWriter.flush();

    await supabase.from('unipile_sequence_executions')
      .update({
        status: 'completed',
        completed_at: new Date().toISOString(),
        execution_log: [...executionLog, {
          step_id: currentStep.id,
          step_type: currentStep.step_type,
          action: 'completed_connection_status',
          status: 'completed',
          reason: completedReason,
          executed_at: new Date().toISOString(),
        }],
        updated_at: new Date().toISOString(),
      })
      .eq('id', execution_id);

    console.log(`✅ Execution ${execution_id} completed (connection status: ${completedReason})`);
    return;
  }

  // 12. Record step result
  const stepSuccess = !stepError && stepResult?.success !== false;
  const resultRecord: any = {
    execution_id,
    step_id: currentStep.id,
    lead_id: (execution as any).lead_id,
    contact_id: (execution as any).contact_id,
    step_type: currentStep.step_type,
    status: stepSuccess ? 'success' : 'failed',
    error_message: stepError || null,
    unipile_message_id: stepResult?.provider_id || stepResult?.message_id || null,
    unipile_chat_id: stepResult?.chat_id || null,
    executed_at: executedAt || new Date().toISOString(),
  };

  if (currentStep.step_type === 'email' && stepResult) {
    resultRecord.tracking_id = stepResult.tracking_id || null;
    resultRecord.unipile_message_id = stepResult.provider_id || null;
    resultRecord.response_data = {
      provider_id: stepResult.provider_id,
      tracking_id: stepResult.tracking_id,
      subject: stepResult.subject,
      is_follow_up: !!stepResult.was_reply,
      reply_to: stepResult.in_reply_to_message_id,
    };
  }

  if (currentStep.step_type === 'linkedin_invitation') {
    resultRecord.unipile_message_id = stepResult?.invitation_id || null;
    resultRecord.response_data = {
      invitation_id: stepResult?.invitation_id || null,
      chat_id: stepResult?.chat_id || null,
      provider_id: stepResult?.provider_id || null,
      public_identifier: stepResult?.public_identifier || null,
      resolved_profile_url: stepResult?.resolved_profile_url || null,
      sent_from_unipile_account: stepResult?.sent_from_unipile_account || null,
      personalized_message: stepResult?.personalized_message || null,
      ...(stepError ? { error: stepError } : {}),
    };
  }

  if (currentStep.step_type === 'linkedin_engage_post') {
    resultRecord.response_data = {
      provider_id: stepResult?.provider_id || null,
      post_id: stepResult?.post_id || null,
      action: stepResult?.action || null,
      post_preview: stepResult?.post_preview || null,
      ...(stepResult?.comment_text ? { comment_text: stepResult.comment_text } : {}),
      ...(stepResult?.skipped ? { skipped: true, reason: stepResult.reason } : {}),
      ...(stepError ? { error: stepError } : {}),
    };
  }

  if (currentStep.step_type === 'linkedin_message') {
    resultRecord.unipile_message_id = stepResult?.message_id || stepResult?.invitation_id || null;
    resultRecord.response_data = stepResult ? {
      chat_id: stepResult.chat_id || null,
      message_id: stepResult.message_id || null,
      invitation_id: stepResult.invitation_id || null,
      provider_id: stepResult.provider_id || null,
      public_identifier: stepResult.public_identifier || null,
      personalized_message: stepResult.personalized_message || null,
      ...(stepError ? { error: stepError } : {}),
    } : { error: stepError || null };
  }

  await stepResultWriter.add(resultRecord);

  // 13. Rollback daily limit on failure
  if (!stepSuccess && preIncrementedAccountId) {
    try {
      await supabase.rpc('rollback_daily_limit_increment', {
        p_account_id: preIncrementedAccountId,
        p_action_type: preIncrementedMessageType,
        p_date: new Date().toISOString().split('T')[0],
      });
    } catch (e: any) {
      console.warn(`⚠️ Failed to rollback daily limit for ${preIncrementedAccountId}:`, e.message);
    }
  }

  // 14. Insert message_sent event for email steps (powers inbox/unibox display)
  if (stepSuccess && currentStep.step_type === 'email' && stepResult) {
    await supabase.from('unipile_message_events').insert({
      event_type: 'message_sent',
      unipile_sequence_id: (execution as any).unipile_sequence_id,
      execution_id,
      lead_id: (execution as any).lead_id || null,
      contact_id: (execution as any).contact_id || null,
      original_client_id: sequence?.client_id || null,
      receiving_account_id: assignedEmailAccountId,
      unipile_message_id: stepResult.provider_id || stepResult.tracking_id || null,
      unipile_chat_id: null,
      message_text: stepResult.body || null,
      is_read: true,
      event_data: {
        step_type: 'email',
        step_id: currentStep.id,
        subject: stepResult.subject || null,
        provider_id: stepResult.provider_id || null,
        automated: true,
      },
    });
  }

  // 15. Update chat_id on execution if available
  if (stepResult?.chat_id && stepResult.chat_id !== (execution as any).chat_id) {
    await supabase.from('unipile_sequence_executions')
      .update({ chat_id: stepResult.chat_id })
      .eq('id', execution_id);
  }

  // 15. Handle step failure with retry logic
  if (!stepSuccess) {
    const errMsg = stepError || '';
    const attemptsMade = executionLog.filter(
      (l: any) => l.step_id === currentStep.id && l.action === 'attempt'
    ).length;
    const maxRetries = 3;
    const retryable = isRetryableError(errMsg);
    const nonRetryable = isNonRetryableError(errMsg);
    const isLastStep = !(await getNextStepId(currentStep.id));
    const isGating = GATING_STEP_TYPES.includes(currentStep.step_type);

    const isFollowUpRejection = errMsg.includes('follow_up_threading_rejected');

    if (nonRetryable || !retryable || attemptsMade >= maxRetries) {
      if (isFollowUpRejection) {
        await supabase.from('unipile_sequence_executions')
          .update({
            status: 'paused',
            error_message: errMsg,
            execution_log: [...executionLog, {
              step_id: currentStep.id, step_type: currentStep.step_type,
              action: 'follow_up_rejected_paused', error: errMsg,
              executed_at: new Date().toISOString(),
            }],
            updated_at: new Date().toISOString(),
          })
          .eq('id', execution_id);
        console.log(`⏸️ [${execution_id}] Follow-up email rejected (422), pausing for manual review`);
      } else if (isGating && attemptsMade >= maxRetries) {
        await supabase.from('unipile_sequence_executions')
          .update({
            status: 'paused',
            execution_log: [...executionLog, {
              step_id: currentStep.id, step_type: currentStep.step_type,
              action: 'max_retries_gating_paused', error: errMsg,
              executed_at: new Date().toISOString(),
            }],
            updated_at: new Date().toISOString(),
          })
          .eq('id', execution_id);
        console.log(`⏸️ Gating max retries, pausing ${execution_id}`);
      } else if (isLastStep || nonRetryable) {
        await supabase.from('unipile_sequence_executions')
          .update({
            status: 'completed',
            execution_log: [...executionLog, {
              step_id: currentStep.id, step_type: currentStep.step_type,
              action: 'failed_completed', error: errMsg,
              executed_at: new Date().toISOString(),
            }],
            updated_at: new Date().toISOString(),
          })
          .eq('id', execution_id);
      } else {
        const skipNextStepId = await getNextStepId(currentStep.id);
        if (skipNextStepId) {
          const nextExecAt = await enforceTimeWindow(new Date(), sequence);
          await supabase.from('unipile_sequence_executions')
            .update({
              current_step_id: skipNextStepId,
              next_execution_at: nextExecAt.toISOString(),
              execution_log: [...executionLog, {
                step_id: currentStep.id, step_type: currentStep.step_type,
                action: 'failed_skipped_to_next', error: errMsg,
                executed_at: new Date().toISOString(),
              }],
              updated_at: new Date().toISOString(),
            })
            .eq('id', execution_id);
        }
      }
    } else {
      // Schedule retry with exponential backoff + jitter
      const retryDelayMs = Math.pow(2, attemptsMade) * 60_000 + Math.random() * 30_000;
      const retryAt = new Date(Date.now() + retryDelayMs).toISOString();
      await supabase.from('unipile_sequence_executions')
        .update({
          next_execution_at: retryAt,
          execution_log: [...executionLog, {
            step_id: currentStep.id, step_type: currentStep.step_type,
            action: 'attempt', error: errMsg, attempt: attemptsMade + 1,
            executed_at: new Date().toISOString(),
          }],
          updated_at: new Date().toISOString(),
        })
        .eq('id', execution_id);
      console.log(`🔄 Retrying ${execution_id} in ${Math.round(retryDelayMs / 1000)}s (attempt ${attemptsMade + 1}/${maxRetries})`);
    }
    return;
  }

  // 16. Advance to next step
  const logEntry = {
    step_id: currentStep.id,
    step_type: currentStep.step_type,
    action: 'completed',
    executed_at: new Date().toISOString(),
    result: stepResult ? {
      provider_id: stepResult.provider_id,
      chat_id: stepResult.chat_id,
      message_id: stepResult.message_id,
    } : undefined,
  };

  if (!nextStepId) {
    await supabase.from('unipile_sequence_executions')
      .update({
        status: 'completed',
        execution_log: [...executionLog, logEntry],
        updated_at: new Date().toISOString(),
      })
      .eq('id', execution_id);
    console.log(`✅ Execution ${execution_id} completed`);
    return;
  }

  // Pre-apply delay if next step is delay type
  const nextStep = steps.find((s: any) => s.id === nextStepId);
  let nextExecAt: Date = new Date();

  if (nextStep?.step_type === 'delay') {
    const dv = nextStep.configuration?.delay_value || nextStep.delay_value || 1;
    const du = nextStep.configuration?.delay_unit || nextStep.delay_unit || 'days';
    const rawDate = new Date(Date.now() + calculateDelay(dv, du));
    if (du === 'days' && sequence?.scheduled_start_time) {
      const tz = sequence.timezone || 'UTC';
      const [sH, sM] = sequence.scheduled_start_time.split(':').map(Number);
      const targetDayStr = rawDate.toLocaleDateString('en-CA', { timeZone: tz }); // YYYY-MM-DD
      nextExecAt = convertToUTC(targetDayStr, sH, sM, tz);
    } else {
      nextExecAt = rawDate;
    }
  }

  nextExecAt = await enforceTimeWindow(nextExecAt, sequence);

  const advanceUpdate: any = {
    current_step_id: nextStepId,
    next_execution_at: nextExecAt.toISOString(),
    execution_log: [...executionLog, logEntry],
    updated_at: new Date().toISOString(),
  };

  if (stepResult?.chat_id) advanceUpdate.chat_id = stepResult.chat_id;

  await supabase.from('unipile_sequence_executions')
    .update(advanceUpdate)
    .eq('id', execution_id);

  console.log(`➡️ Execution ${execution_id} advanced to step ${nextStepId}`);

  } finally {
    if (claimed) {
      const { error: releaseErr } = await supabase.rpc('release_execution_claim', {
        p_execution_id: execution_id,
        p_new_state: 'not_started',
      });
      if (releaseErr) {
        console.error(`❌ [${execution_id}] release_execution_claim failed: ${releaseErr.message} — forcing execution_state reset`);
        await supabase.from('unipile_sequence_executions')
          .update({ execution_state: 'not_started', updated_at: new Date().toISOString() })
          .eq('id', execution_id);
      }
    }
  }
}

export function startExecutionWorker() {
  const supabaseRef = config.supabase.url.match(/https?:\/\/([^.]+)/)?.[1] || 'unknown';
  console.log(`🔗 Supabase project ref: ${supabaseRef}`);

  const stepResultWriter = new BatchWriter(supabase, 'unipile_step_results', { onConflict: 'execution_id,step_id' });

  const worker = new Worker<ExecutionJobData>(
    'outreach-executions',
    async (job: Job<ExecutionJobData>) => {
      const { execution_id, group_key, channel } = job.data;
      console.log(`🚀 [queue=outreach-executions job=${job.id}] execution=${execution_id} group=${group_key} channel=${channel}`);

      // Per-account LinkedIn pacing: enforce 45-90s gap between consecutive actions
      if (channel === 'linkedin') {
        const accountId = group_key.split(':')[1];
        if (accountId && accountId !== 'unknown') {
          const waitMs = await acquireLinkedInSlot(accountId);
          if (waitMs > 0) {
            const newJobId = `exec-${execution_id}-${Date.now()}`;
            const requeueDelay = Math.round(waitMs) + Math.round(Math.random() * 5_000);
            await executionQueue.add('execute-step', job.data, {
              delay: requeueDelay,
              attempts: 3,
              backoff: { type: 'exponential', delay: 5000 },
              jobId: newJobId,
              removeOnComplete: { age: 3600, count: 1000 },
              removeOnFail: { age: 86400, count: 5000 },
            });
            console.log(`⏳ [linkedin-pacing] exec=${execution_id} account=${accountId} requeued in ${Math.round(waitMs / 1000)}s newJobId=${newJobId}`);
            return;
          }
          console.log(`✅ [linkedin-pacing] exec=${execution_id} account=${accountId} slot acquired waitMs=0`);
        }
      }

      // Per-account email pacing: enforce 10-20s gap between consecutive sends
      if (channel === 'email') {
        const accountId = group_key.split(':')[1];
        if (accountId && accountId !== 'unknown') {
          const waitMs = await acquireEmailSlot(accountId);
          if (waitMs > 0) {
            const newJobId = `exec-${execution_id}-${Date.now()}`;
            await executionQueue.add('execute-step', job.data, {
              delay: Math.round(waitMs),
              attempts: 3,
              backoff: { type: 'exponential', delay: 5000 },
              jobId: newJobId,
              removeOnComplete: { age: 3600, count: 1000 },
              removeOnFail: { age: 86400, count: 5000 },
            });
            console.log(`⏳ [email-pacing] exec=${execution_id} account=${accountId} requeued in ${Math.round(waitMs / 1000)}s newJobId=${newJobId}`);
            return;
          }
          console.log(`✅ [email-pacing] exec=${execution_id} account=${accountId} slot acquired waitMs=0`);
        }
      }

      try {
        await executeStep(execution_id, stepResultWriter, job);
      } catch (err: any) {
        console.error(`❌ [job=${job.id}] Fatal error in executeStep for ${execution_id}:`, err.message);

        // Outer catch: ensure next_execution_at is set to the future so the scanner
        // does not loop. Uses retry_count (the actual column name in the schema).
        const { data: exec, error: fetchErr } = await supabase
          .from('unipile_sequence_executions')
          .select('status, retry_count')
          .eq('id', execution_id)
          .single();

        if (fetchErr) {
          console.error(`❌ [job=${job.id}] Could not re-fetch execution ${execution_id} for retry scheduling:`, fetchErr.message);
        }

        if (exec?.status === 'running') {
          const retryCount = ((exec as any).retry_count || 0) + 1;
          const retryDelayMs = Math.pow(2, Math.min(retryCount - 1, 3)) * 60_000 + Math.random() * 30_000;
          await supabase.from('unipile_sequence_executions')
            .update({
              next_execution_at: new Date(Date.now() + retryDelayMs).toISOString(),
              retry_count: retryCount,
              error_message: err.message,
              updated_at: new Date().toISOString(),
            })
            .eq('id', execution_id);
          console.log(`🔄 [job=${job.id}] Outer catch: retry ${execution_id} in ${Math.round(retryDelayMs / 1000)}s (retry_count=${retryCount})`);
        }

        throw err;
      }
    },
    {
      connection,
      concurrency: config.workerConcurrency,
      limiter: {
        max: config.workerRateLimit,
        duration: 60_000,
      },
    }
  );

  worker.on('active', (job) => {
    console.log(`▶️  [queue=outreach-executions] job=${job.id} active`);
  });

  worker.on('failed', (job, err) => {
    console.error(`❌ [queue=outreach-executions] job=${job?.id} failed (attempt ${job?.attemptsMade}/${job?.opts?.attempts}):`, err.message);
  });

  worker.on('completed', (job) => {
    console.log(`✅ [queue=outreach-executions] job=${job.id} completed`);
  });

  console.log(`✅ BullMQ processor listening on queue 'outreach-executions' (concurrency=${config.workerConcurrency}, rate=${config.workerRateLimit}/min)`);
  return worker;
}
