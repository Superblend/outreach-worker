/**
 * Core orchestrator scheduling logic.
 *
 * Triggered by a wake event (Realtime, poll-fallback, or startup scan), this
 * module decides what to do for one sequence:
 *
 *   1. Read sequence + client. Skip if client is `legacy` or sequence
 *      isn't `use_bullmq=true`.
 *   2. Validate active window / active days in sequence's timezone.
 *   3. Compute today's local date in that timezone — this is the slot date.
 *   4. Iterate cohorts in priority order: P1 → P2 → P3 → P4.
 *      For each cohort:
 *        - Fetch pending executions matching the cohort criteria.
 *        - For each candidate: check/claim slot (P2 same-day usually free).
 *        - If client is `shadow`: log the decision, do not enqueue.
 *        - If client is `orchestrator`: enqueue to the correct BullMQ queue,
 *          using the same patterns scanner.ts uses (per-account LinkedIn,
 *          per-client email, shared otherwise).
 *
 * IMPLEMENTATION NOTE (review): the heavy bits — cohort filtering, time
 * window validation, BullMQ jobId pattern — are deliberately stubbed here.
 * They'll be ported / shared with `scanner.ts` once the scaffold is approved.
 * The point of this file in the scaffold is the *flow*, not the SQL.
 */

import { supabase } from '../supabase';
import { getOrchestratorMode } from './mode-reader';
import { countSlotsForDate, hasSlot, claimSlot, todayInSequenceTz } from './slot-manager';
import { recordShadowDecision } from './shadow-logger';
import type {
  OrchDecision,
  OrchSequenceWakeEvent,
  OrchSkipReason,
  OrchestratorMode,
} from './types';

/**
 * Entry point called by the Realtime subscriber and poll fallback.
 * One call = one sequence considered.
 */
export async function handleWakeEvent(event: OrchSequenceWakeEvent): Promise<void> {
  // 1. Load sequence + client metadata.
  const seq = await loadSequence(event.sequenceId);
  if (!seq) return;

  // Out-of-scope: legacy clients & non-bullmq sequences are handled by the
  // existing edge-function dispatcher + worker scanner.
  if (!seq.use_bullmq) {
    return logSkip(event, seq.id, seq.client_id, 'not_bullmq_sequence');
  }

  const mode = await getOrchestratorMode(seq.client_id);
  if (mode === 'legacy') {
    return logSkip(event, seq.id, seq.client_id, 'client_legacy_mode');
  }

  // 2. Sequence-level gates: status + active window + active days.
  if (seq.status !== 'active') {
    return logSkip(event, seq.id, seq.client_id, 'sequence_not_active');
  }

  const localDate = todayInSequenceTz(seq.timezone);
  if (!isActiveDayAndWindow(seq, new Date())) {
    return logSkip(
      event,
      seq.id,
      seq.client_id,
      'outside_active_window' /* or 'inactive_day' — refine in impl */,
    );
  }

  // 3. Daily slot budget check.
  const slotsUsed = await countSlotsForDate(seq.id, localDate);
  const slotsAvailable = Math.max(0, (seq.daily_batch_size ?? 0) - slotsUsed);

  // 4. Cohort loop — TODO in impl. The order MUST match scanner.ts's
  //    cohortPriority() so shadow comparison can pair decisions.
  //
  //    Pseudocode:
  //      for cohort of [P1_new, P2_same_day, P3_multi_day, P4_low]:
  //        candidates = fetchCohortCandidates(seq, cohort, localDate);
  //        for candidate in candidates:
  //          consumesSlot = !(await hasSlot(seq.id, candidate.lead_id, localDate));
  //          if (consumesSlot && slotsAvailable <= 0) {
  //            recordShadowDecision({ ..., skipReason: 'daily_limit_reached' });
  //            continue;
  //          }
  //          if (consumesSlot) {
  //            const claimed = await claimSlot(seq.id, candidate.lead_id, localDate);
  //            if (!claimed) {
  //              // Someone else won the race; skip this candidate.
  //              continue;
  //            }
  //            slotsAvailable--;
  //          }
  //          const decision = buildDecision(...);
  //          if (mode === 'shadow') {
  //            await recordShadowDecision(decision, event);
  //          } else {
  //            await enqueueExecution(decision);
  //          }

  // Stub for the scaffold: emit a noop log entry so we can see wakes in logs.
  console.log(
    `[scheduler] wake sequence=${seq.id} client=${seq.client_id} mode=${mode} ` +
      `slotsUsed=${slotsUsed} slotsAvailable=${slotsAvailable} source=${event.source}`,
  );
}

interface SequenceMeta {
  id: string;
  client_id: string;
  status: string;
  use_bullmq: boolean;
  timezone: string;
  daily_batch_size: number | null;
  scheduled_start_time: string | null;
  scheduled_end_time: string | null;
  active_days: number[] | null;
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
  return data as SequenceMeta;
}

/**
 * STUB: validate that "now" falls inside the sequence's active window in its
 * local timezone, and that today is one of the configured active days.
 *
 * Real impl reuses the formatToParts-based helpers from scanner.ts /
 * unipile-sync-sequence-leads/index.ts (utcInstantForLocal, localMinutesOfDay,
 * localWeekday) — same logic, lifted into a shared lib.
 */
function isActiveDayAndWindow(_seq: SequenceMeta, _now: Date): boolean {
  return true;
}

/**
 * Shadow-mode courtesy: even skip reasons get logged in shadow mode so
 * comparison can confirm "old path agreed this was skipped too."
 * In orchestrator mode, skips just get a console line.
 */
async function logSkip(
  event: OrchSequenceWakeEvent,
  sequenceId: string,
  clientId: string,
  reason: OrchSkipReason,
): Promise<void> {
  const mode: OrchestratorMode = await getOrchestratorMode(clientId).catch(() => 'legacy');
  if (mode !== 'shadow') {
    console.log(`[scheduler] skip sequence=${sequenceId} reason=${reason}`);
    return;
  }
  // In shadow we still want a row so divergence analysis sees "we skipped X".
  const stubDecision: OrchDecision = {
    sequenceId,
    clientId,
    leadId: '',
    executionId: null,
    stepId: '',
    cohortPriority: 0,
    cohortLabel: 'p4_low_priority',
    nextExecutionAt: event.observedAt,
    consumesSlot: false,
    skipReason: reason,
  };
  await recordShadowDecision(stubDecision, event);
}

// Stub helpers exported for testing & future implementation. Intentionally
// not implemented in the scaffold to keep the surface small.
export const __TESTING__ = { hasSlot, claimSlot };
