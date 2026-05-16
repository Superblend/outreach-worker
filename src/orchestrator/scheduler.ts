/**
 * Core orchestrator scheduling logic.
 *
 * Triggered by a wake event (Realtime, poll-fallback, or startup scan), this
 * module decides what to do for one sequence. Output is one or more
 * `OrchDecision` rows written to `orchestrator_shadow_log` (shadow mode).
 *
 * SAFETY: `orchestrator` mode enqueue is intentionally NOT implemented in
 * this file. Until the enqueue path is wired and tested in a follow-up
 * commit, this scheduler refuses to act for clients in `orchestrator` mode
 * and emits a loud error. Do NOT flip any client to `orchestrator` until
 * that path is shipped. `shadow` mode works fully.
 *
 * Invariants preserved (mirrors scanner.ts):
 *   - 30-second `updated_at` buffer to avoid races with concurrent workers.
 *   - Cohort priority order from `cohortPriority()` (ported from scanner.ts).
 *   - Active window / active days enforcement via `isWithinActiveWindow`.
 *   - Slot logic lives INSIDE the cohort loop, not in place of it.
 *   - Account assignment is NOT done here — orchestrator emits decisions
 *     with `assigned_*_account_id = NULL`; worker assigns at dispatch time.
 */

import { supabase } from '../supabase';
import { isWithinActiveWindow } from '../lib/time-utils';
import { getOrchestratorMode } from './mode-reader';
import { countSlotsForDate, hasSlot, claimSlot, todayInSequenceTz } from './slot-manager';
import { recordShadowDecision } from './shadow-logger';
import type {
  OrchCohortLabel,
  OrchDecision,
  OrchSequenceWakeEvent,
  OrchSkipReason,
  OrchestratorMode,
} from './types';

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

/** Max executions considered per wake event. Per-sequence, so small budget. */
const MAX_CANDIDATES_PER_WAKE = 500;

/** Mirrors scanner.ts's updatedAtBuffer — race guard against concurrent workers. */
const UPDATED_AT_BUFFER_MS = 30_000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Entry point called by the Realtime subscriber and poll fallback.
 * One call = one sequence considered.
 */
export async function handleWakeEvent(event: OrchSequenceWakeEvent): Promise<void> {
  // 1. Load sequence + client metadata.
  const seq = await loadSequence(event.sequenceId);
  if (!seq) return;

  // Out-of-scope: legacy sequences are handled by the existing edge-function
  // dispatcher. Orchestrator only owns use_bullmq=true sequences.
  if (!seq.use_bullmq) {
    return; // silent — this is a normal "not for us"
  }

  const mode = await getOrchestratorMode(seq.client_id);
  if (mode === 'legacy') {
    return; // not for us, scanner handles
  }

  // 2. Sequence-level gates.
  if (seq.status !== 'active') {
    return logSkip(event, seq, null, 'sequence_not_active', mode);
  }

  const windowCheck = isWithinActiveWindow(
    {
      timezone: seq.timezone,
      active_days: seq.active_days,
      scheduled_start_time: seq.scheduled_start_time,
      scheduled_end_time: seq.scheduled_end_time,
    },
    new Date(),
  );
  if (!windowCheck.ok) {
    const reasonStr = (windowCheck as { ok: false; reason: string }).reason;
    const reason: OrchSkipReason = reasonStr.startsWith('inactive_day')
      ? 'inactive_day'
      : 'outside_active_window';
    return logSkip(event, seq, null, reason, mode);
  }

  // 3. Daily slot budget for today (sequence-local date).
  const localDate = todayInSequenceTz(seq.timezone ?? 'UTC');
  const slotsUsed = await countSlotsForDate(seq.id, localDate);
  const dailyBudget = seq.daily_batch_size ?? 0;
  let slotsAvailable = Math.max(0, dailyBudget - slotsUsed);

  // 4. Fetch due executions for this sequence.
  const candidates = await fetchDueExecutions(seq.id);
  if (candidates.length === 0) {
    console.log(`[scheduler] wake sequence=${seq.id} mode=${mode} due=0 source=${event.source}`);
    return;
  }

  // 5. Sort by cohort priority (ascending — lower number = higher priority).
  //    Mirrors scanner.ts's cohortPriority() exactly so shadow comparisons
  //    pair correctly with legacy decisions.
  const now = Date.now();
  const sorted = candidates
    .map((c) => ({
      exec: c,
      priority: cohortPriority(c.priority_cohort, c.first_touch_done, c.next_execution_at, now),
    }))
    .sort((a, b) => a.priority - b.priority);

  console.log(
    `[scheduler] wake sequence=${seq.id} mode=${mode} due=${sorted.length} ` +
      `slotsUsed=${slotsUsed}/${dailyBudget} source=${event.source}`,
  );

  // 6. Iterate candidates in cohort order. For each, decide slot need,
  //    claim if necessary, emit decision.
  let decisionsRecorded = 0;
  let dailyLimitReached = false;

  for (const { exec, priority } of sorted) {
    const label = cohortLabelFromPriority(priority);

    // Same-day chain (P2): lead already has a slot today → free continuation.
    let consumesSlot: boolean;
    try {
      consumesSlot = !(await hasSlot(seq.id, exec.lead_id, localDate));
    } catch {
      // Slot read failed — fail closed by skipping this candidate this pass.
      continue;
    }

    if (consumesSlot && slotsAvailable <= 0) {
      // Budget exhausted. Record one skip-decision per remaining candidate
      // so shadow comparison can confirm "we agreed with legacy on the cap".
      // Then stop scanning (no later candidate can claim a slot either).
      await emitDecision(
        {
          sequenceId: seq.id,
          clientId: seq.client_id,
          leadId: exec.lead_id,
          executionId: exec.id,
          stepId: exec.current_step_id,
          cohortPriority: priority,
          cohortLabel: label,
          nextExecutionAt: exec.next_execution_at,
          consumesSlot: false, // didn't claim
          skipReason: 'daily_limit_reached',
        },
        event,
        mode,
      );
      decisionsRecorded++;
      dailyLimitReached = true;
      continue;
    }

    if (consumesSlot) {
      const claimed = await claimSlot(seq.id, exec.lead_id, localDate);
      if (!claimed) {
        // Race: another orchestrator pass beat us to it. Skip silently — the
        // winning pass already recorded a decision for this lead.
        continue;
      }
      slotsAvailable--;
    }

    await emitDecision(
      {
        sequenceId: seq.id,
        clientId: seq.client_id,
        leadId: exec.lead_id,
        executionId: exec.id,
        stepId: exec.current_step_id,
        cohortPriority: priority,
        cohortLabel: label,
        nextExecutionAt: exec.next_execution_at,
        consumesSlot,
      },
      event,
      mode,
    );
    decisionsRecorded++;
  }

  console.log(
    `[scheduler] sequence=${seq.id} mode=${mode} decisions=${decisionsRecorded}` +
      (dailyLimitReached ? ' (daily_limit_reached during pass)' : ''),
  );
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

interface SequenceMeta {
  id: string;
  client_id: string;
  status: string;
  use_bullmq: boolean;
  timezone: string | null;
  daily_batch_size: number | null;
  scheduled_start_time: string | null;
  scheduled_end_time: string | null;
  active_days: number[] | null;
}

interface ExecutionCandidate {
  id: string;
  lead_id: string;
  current_step_id: string;
  batch_number: number | null;
  next_execution_at: string;
  priority_cohort: string | null;
  first_touch_done: boolean;
}

async function loadSequence(sequenceId: string): Promise<SequenceMeta | null> {
  const { data, error } = await supabase
    .from('unipile_sequences')
    .select(
      'id, client_id, status, use_bullmq, timezone, daily_batch_size, ' +
        'scheduled_start_time, scheduled_end_time, active_days',
    )
    .eq('id', sequenceId)
    .maybeSingle();
  if (error || !data) return null;
  return data as unknown as SequenceMeta;
}

/**
 * Fetch executions due now for this sequence.
 *
 * Single query (per-sequence volume is bounded by daily_batch_size, typically
 * dozens). Sorting by cohort priority happens in-memory after the fetch.
 *
 * Filters:
 *   - status='running' AND execution_state='not_started' (matches scanner.ts)
 *   - next_execution_at <= now (due now or overdue)
 *   - updated_at < now - 30s (race guard against concurrent workers / passes)
 */
async function fetchDueExecutions(sequenceId: string): Promise<ExecutionCandidate[]> {
  const nowIso = new Date().toISOString();
  const updatedAtBuffer = new Date(Date.now() - UPDATED_AT_BUFFER_MS).toISOString();

  const { data, error } = await supabase
    .from('unipile_sequence_executions')
    .select(
      'id, lead_id, current_step_id, batch_number, next_execution_at, ' +
        'priority_cohort, first_touch_done',
    )
    .eq('unipile_sequence_id', sequenceId)
    .eq('status', 'running')
    .eq('execution_state', 'not_started')
    .lte('next_execution_at', nowIso)
    .lt('updated_at', updatedAtBuffer)
    .order('batch_number', { ascending: true, nullsFirst: true })
    .order('next_execution_at', { ascending: true })
    .limit(MAX_CANDIDATES_PER_WAKE);

  if (error) {
    console.error(`[scheduler] fetch failed for sequence=${sequenceId}:`, error.message);
    return [];
  }
  return (data ?? []) as unknown as ExecutionCandidate[];
}

/**
 * Ported verbatim from scanner.ts. Lower number = higher BullMQ priority.
 * Keeping the exact integer mapping is what lets shadow-mode comparison
 * pair our decisions with the legacy scanner's enqueues.
 */
function cohortPriority(
  cohort: string | null,
  firstTouchDone: boolean,
  nextExecutionAt: string,
  now: number = Date.now(),
): number {
  if (cohort === 'in_flight') {
    const overdueMs = now - new Date(nextExecutionAt).getTime();
    if (overdueMs > 86_400_000) return 1;
    if (overdueMs > 3_600_000) return 2;
    if (overdueMs > 900_000) return 3;
    return 6;
  }
  if (cohort === 'new_today') return firstTouchDone ? 5 : 4;
  return 7;
}

/**
 * Map the numeric priority into a human label for the shadow log.
 * The numeric value is the authoritative input to BullMQ; the label is
 * for analysis queries.
 */
function cohortLabelFromPriority(priority: number): OrchCohortLabel {
  if (priority <= 3) return 'p3_multi_day_follow_up'; // overdue in_flight buckets
  if (priority === 4) return 'p1_new_contact';
  if (priority === 5) return 'p2_same_day_chain';
  if (priority === 6) return 'p2_same_day_chain'; // on-time in_flight, still same-day class
  return 'p4_low_priority';
}

/**
 * Emit a decision through the right path for the current mode.
 *
 *   shadow      → write to orchestrator_shadow_log, that's it.
 *   orchestrator → ENQUEUE PATH NOT YET IMPLEMENTED. Logs an error and skips.
 *                  Until this path is shipped, no client should be in
 *                  orchestrator mode in any environment. The mode-reader
 *                  CHECK constraint allows the value; the scaffold-time guard
 *                  is here, in the dispatch logic. Belt + suspenders.
 *   legacy      → unreachable (we returned earlier).
 */
async function emitDecision(
  decision: OrchDecision,
  event: OrchSequenceWakeEvent,
  mode: OrchestratorMode,
): Promise<void> {
  if (mode === 'shadow') {
    await recordShadowDecision(decision, event);
    return;
  }
  if (mode === 'orchestrator') {
    console.error(
      `[scheduler] REFUSING ENQUEUE — orchestrator-mode enqueue not yet shipped. ` +
        `sequence=${decision.sequenceId} execution=${decision.executionId} ` +
        `lead=${decision.leadId}. Set client back to 'shadow' until enqueue lands.`,
    );
    return;
  }
}

/**
 * Shadow-mode courtesy: even skip reasons get logged in shadow so comparison
 * can confirm "old path agreed this was skipped too." In orchestrator mode
 * (currently unreachable for enqueues), skips just go to the console.
 */
async function logSkip(
  event: OrchSequenceWakeEvent,
  seq: SequenceMeta,
  exec: ExecutionCandidate | null,
  reason: OrchSkipReason,
  mode: OrchestratorMode,
): Promise<void> {
  if (mode !== 'shadow') {
    console.log(`[scheduler] skip sequence=${seq.id} reason=${reason}`);
    return;
  }
  const stub: OrchDecision = {
    sequenceId: seq.id,
    clientId: seq.client_id,
    leadId: exec?.lead_id ?? '',
    executionId: exec?.id ?? null,
    stepId: exec?.current_step_id ?? '',
    cohortPriority: 0,
    cohortLabel: 'p4_low_priority',
    nextExecutionAt: exec?.next_execution_at ?? event.observedAt,
    consumesSlot: false,
    skipReason: reason,
  };
  await recordShadowDecision(stub, event);
}

// Exported for testing — keeps the file lean while letting tests poke internals.
export const __TESTING__ = { cohortPriority, cohortLabelFromPriority, fetchDueExecutions };
