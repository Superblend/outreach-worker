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

import { randomBytes } from 'crypto';
import { supabase, invokeEdgeFunction } from '../supabase';
import { isWithinActiveWindow } from '../lib/time-utils';
import { dispatchPendingQueue } from '../queues/definitions';
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

/**
 * Per-process session ID for BullMQ jobId stability. Stable within a session
 * (dedupes waiting/delayed jobs), unique across restarts (prevents stale
 * failed jobs from blocking new enqueues with the same execution+step pair).
 */
const ORCH_SESSION_ID = randomBytes(4).toString('hex');

/**
 * Per-sequence debouncer for new-lead batch materialization. Prevents the
 * orchestrator from invoking `unipile-process-batch-queue` on every Realtime
 * event (could fire many times per second during burst activity).
 */
const lastMaterializeAt = new Map<string, number>();
const MATERIALIZE_COOLDOWN_MS = 30_000;

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

/** Max executions considered per wake event. Per-sequence, so small budget. */
const MAX_CANDIDATES_PER_WAKE = 500;

// Note: orchestrator does NOT use scanner.ts's 30-second updated_at buffer.
// That buffer protects scanner from cadence-based double-enqueue (15s loop).
// The orchestrator runs reactively via Realtime / poll fallback, so the same
// risk doesn't apply. When orchestrator-mode enqueue lands, BullMQ jobId
// dedupe (jobId = exec_id + step_id + session_id) catches any cross-process
// races between scanner and orchestrator during canary.

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
    // Subject of this execution: prefer contact_id (current model);
    // fall back to lead_id for legacy executions. If neither is set we
    // can't track slot ownership — skip safely.
    const subjectId = exec.contact_id ?? exec.lead_id;
    if (!subjectId) {
      console.log(
        `[scheduler] skip execution=${exec.id} reason=no_subject_id ` +
          `(both contact_id and lead_id are NULL — cannot reserve a slot)`,
      );
      continue;
    }

    // Same-day chain (P2): contact already has a slot today → free continuation.
    let consumesSlot: boolean;
    try {
      consumesSlot = !(await hasSlot(seq.id, subjectId, localDate));
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
          contactId: subjectId,
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
        exec,
      );
      decisionsRecorded++;
      dailyLimitReached = true;
      continue;
    }

    if (consumesSlot) {
      const claimed = await claimSlot(seq.id, subjectId, localDate);
      if (!claimed) {
        // Race: another orchestrator pass beat us to it. Skip silently — the
        // winning pass already recorded a decision for this contact.
        continue;
      }
      slotsAvailable--;
    }

    await emitDecision(
      {
        sequenceId: seq.id,
        clientId: seq.client_id,
        contactId: subjectId,
        executionId: exec.id,
        stepId: exec.current_step_id,
        cohortPriority: priority,
        cohortLabel: label,
        nextExecutionAt: exec.next_execution_at,
        consumesSlot,
      },
      event,
      mode,
      exec,
    );
    decisionsRecorded++;
  }

  console.log(
    `[scheduler] sequence=${seq.id} mode=${mode} decisions=${decisionsRecorded}` +
      (dailyLimitReached ? ' (daily_limit_reached during pass)' : ''),
  );

  // 7. New-lead materialization: if there are due batches sitting in
  //    `unipile_batch_queue` for this sequence, ask the existing
  //    `unipile-process-batch-queue` edge function to materialize them into
  //    executions. The edge function already handles dedup, account rotation,
  //    active-day filter, and INSERT shape — we just decide WHEN to trigger
  //    it. Replaces the daily 09 UTC cron's role for orchestrator-mode clients.
  //
  //    For shadow mode we don't materialize, because doing so would create
  //    real executions that the legacy scanner would then dispatch — defeating
  //    the "observe only" promise of shadow.
  if (mode === 'orchestrator') {
    await materializeDueBatches(seq);
  }
}

/**
 * Trigger `unipile-process-batch-queue` if there are due batches for this
 * sequence and we haven't called it recently (per-sequence debouncer).
 *
 * The edge function processes ALL due batches for the given client_id in one
 * pass. We invoke it per-client (not per-sequence) so a client with many
 * sequences only generates one invocation per cooldown window.
 */
async function materializeDueBatches(seq: SequenceMeta): Promise<void> {
  const key = `client:${seq.client_id}`;
  const last = lastMaterializeAt.get(key) ?? 0;
  const now = Date.now();
  if (now - last < MATERIALIZE_COOLDOWN_MS) return;

  // Are there actually due batches for this sequence? Cheap pre-check so we
  // don't invoke the edge function speculatively on every wake.
  const { data: dueBatches, error } = await supabase
    .from('unipile_batch_queue')
    .select('id', { head: false })
    .eq('unipile_sequence_id', seq.id)
    .eq('status', 'pending')
    .lte('scheduled_for', new Date().toISOString())
    .limit(1);

  if (error) {
    console.warn(`[orchestrator] materialize pre-check failed for sequence=${seq.id}:`, error.message);
    return;
  }
  if (!dueBatches || dueBatches.length === 0) return;

  // Mark cooldown BEFORE the call so concurrent wakes don't pile on.
  lastMaterializeAt.set(key, now);

  console.log(`[orchestrator] triggering batch materialization for client=${seq.client_id} (sequence ${seq.id} has due batches)`);
  const { error: invokeErr } = await invokeEdgeFunction('unipile-process-batch-queue', {
    client_id: seq.client_id,
  });

  if (invokeErr) {
    console.error(`[orchestrator] batch materialization invoke failed for client=${seq.client_id}:`, invokeErr.message);
    // Reset cooldown on failure so the next wake can retry sooner.
    lastMaterializeAt.delete(key);
  } else {
    console.log(`[orchestrator] batch materialization completed for client=${seq.client_id}`);
  }
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
  contact_id: string | null;
  lead_id: string | null;
  current_step_id: string;
  batch_number: number | null;
  next_execution_at: string;
  priority_cohort: string | null;
  first_touch_done: boolean;
  assigned_linkedin_account_id: string | null;
  assigned_email_account_id: string | null;
  // Joined from unipile_sequence_steps. PostgREST returns either an object or
  // a single-element array depending on cardinality; we normalize at use site.
  unipile_sequence_steps: { step_type: string } | { step_type: string }[] | null;
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
 *
 * No updated_at race guard (see note at the top of this file). When
 * orchestrator-mode enqueue lands, BullMQ jobId dedupe protects against
 * any cross-process race with scanner.ts.
 */
async function fetchDueExecutions(sequenceId: string): Promise<ExecutionCandidate[]> {
  const nowIso = new Date().toISOString();

  const { data, error } = await supabase
    .from('unipile_sequence_executions')
    .select(`
      id, contact_id, lead_id, current_step_id, batch_number, next_execution_at,
      priority_cohort, first_touch_done,
      assigned_linkedin_account_id, assigned_email_account_id,
      unipile_sequence_steps!unipile_sequence_executions_current_step_id_fkey(step_type)
    `)
    .eq('unipile_sequence_id', sequenceId)
    .eq('status', 'running')
    .eq('execution_state', 'not_started')
    .lte('next_execution_at', nowIso)
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
 *   orchestrator → enqueue to the right BullMQ queue (per-account LinkedIn,
 *                  per-client email, or shared `outreach-executions`).
 *                  Also touches the execution row's updated_at so the
 *                  scanner-side cadence dedupe doesn't re-pick it.
 *   legacy      → unreachable (we returned earlier).
 *
 * For skip-decisions (skipReason set), we never enqueue regardless of mode —
 * a skip is by definition "do nothing now."
 */
async function emitDecision(
  decision: OrchDecision,
  event: OrchSequenceWakeEvent,
  mode: OrchestratorMode,
  candidate: ExecutionCandidate,
): Promise<void> {
  if (mode === 'shadow') {
    await recordShadowDecision(decision, event);
    return;
  }
  if (mode === 'orchestrator') {
    if (decision.skipReason) {
      // Skip decisions in orchestrator mode are silent except in logs.
      // The corresponding execution remains "running, not_started" and will
      // be re-evaluated on the next wake.
      console.log(
        `[orchestrator] skip execution=${decision.executionId} ` +
          `reason=${decision.skipReason}`,
      );
      return;
    }
    await enqueueExecution(decision, candidate);
    return;
  }
}

/**
 * Two-tier dispatch: push a minimal decision to `outreach-dispatch-pending`.
 *
 * The router worker (src/workers/router.worker.ts) consumes this queue,
 * resolves step_type + assigned accounts from the DB, and re-enqueues to the
 * appropriate per-account (LinkedIn) or per-client (Email) queue. Keeping
 * queue topology in the worker process matches the doc's contract:
 * orchestrator decides WHEN, worker decides HOW.
 *
 * jobId = `exec-{exec_id}-{step_id}-{orch_session}`
 *   - Stable within a session → BullMQ dedupes when the orchestrator
 *     re-emits a decision for the same (exec, step) on subsequent wakes.
 *   - Session suffix rotates on restart → old failed/stale jobs from a prior
 *     orchestrator process don't block new enqueues.
 *
 * We do NOT touch updated_at after enqueue. Scanner already skips
 * orchestrator-mode clients at the client_id level, so the row-level
 * race-guard touch is unnecessary. Touching previously caused a
 * self-triggered Realtime loop.
 */
async function enqueueExecution(
  decision: OrchDecision,
  _candidate: ExecutionCandidate,
): Promise<void> {
  const jobId = `exec-${decision.executionId}-${decision.stepId}-${ORCH_SESSION_ID}`;

  try {
    await dispatchPendingQueue.add(
      'route-dispatch',
      {
        execution_id: decision.executionId,
        step_id: decision.stepId,
        client_id: decision.clientId,
        contact_id: decision.contactId,
        cohort_priority: decision.cohortPriority,
        cohort_label: decision.cohortLabel,
      },
      {
        attempts: 3,
        backoff: { type: 'exponential', delay: 2000 },
        jobId,
        priority: decision.cohortPriority,
        removeOnComplete: { age: 3600, count: 1000 },
        removeOnFail: { age: 86400, count: 5000 },
      },
    );
    console.log(
      `[orchestrator] dispatched exec=${decision.executionId} ` +
        `step=${decision.stepId} priority=${decision.cohortPriority} ` +
        `cohort=${decision.cohortLabel}`,
    );
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(
      `[orchestrator] dispatch enqueue failed exec=${decision.executionId}: ${msg}`,
    );
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
    contactId: exec?.contact_id ?? exec?.lead_id ?? '',
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
