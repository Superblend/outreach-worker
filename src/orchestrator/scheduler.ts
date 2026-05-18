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
import { supabase } from '../supabase';
import { isWithinActiveWindow } from '../lib/time-utils';
import { dispatchPendingQueue } from '../queues/definitions';
import { getOrchestratorMode } from './mode-reader';
import { countSlotsForDate, hasSlot, claimSlot, todayInSequenceTz } from './slot-manager';
import { recordShadowDecision } from './shadow-logger';
import { tryReserveDispatchSlot, releaseDispatchSlot } from './dispatch-budget';
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

/**
 * Short-lived cap-read cache. The pre-flight cap check fires once per
 * dispatch decision; without caching, a single wake on a sequence with N
 * candidates would issue N DB reads against `account_daily_limits`. The
 * cache collapses repeat reads for the same account inside one wake (and
 * the next few wakes within the TTL window).
 *
 * 10s TTL is short enough that a cap raise or another sequence's send
 * filling headroom shows up within seconds, but long enough that bursts
 * of dispatch decisions read the DB at most once per account per cycle.
 * The worker's atomic cap RPC is still the source of truth at send time,
 * so a brief cache miss can't cause an actual overshoot.
 */
interface CapCacheEntry {
  sent: number;
  max: number;
  expiresAt: number;
}
const CAP_CACHE_TTL_MS = 10_000;
const capCache = new Map<string, CapCacheEntry>();

type CapChannel = 'email' | 'linkedin_invitation' | 'linkedin_message';

const LINKEDIN_INVITATION_TYPES = new Set(['linkedin_invitation']);
const LINKEDIN_MESSAGE_TYPES = new Set([
  'linkedin_message',
  'linkedin_voice_note',
  'linkedin_engage_post',
  'linkedin_endorse',
  'linkedin_profile_visit',
]);

function capChannelForStep(stepType: string | null | undefined): CapChannel | null {
  if (!stepType) return null;
  if (stepType === 'email') return 'email';
  if (LINKEDIN_INVITATION_TYPES.has(stepType)) return 'linkedin_invitation';
  if (LINKEDIN_MESSAGE_TYPES.has(stepType)) return 'linkedin_message';
  return null; // delay, conditional — no cap, doesn't pre-flight
}

/**
 * Returns true if the assigned account is at its per-day cap for this
 * channel. Uses a 10s in-process cache to avoid re-reading the same account
 * for every candidate in a wake.
 *
 * Fail-open: if the cap row is missing or the read errors, return false
 * (i.e. let the worker make the decision atomically). The worker's RPC is
 * the real gate; this check is just an optimization.
 */
async function accountAtCap(accountId: string, channel: CapChannel): Promise<boolean> {
  const cacheKey = `${channel}:${accountId}`;
  const now = Date.now();
  const cached = capCache.get(cacheKey);
  if (cached && cached.expiresAt > now) {
    return cached.sent >= cached.max && cached.max > 0;
  }

  const today = new Date().toISOString().slice(0, 10);
  const { sentCol, maxCol } = (() => {
    if (channel === 'email') return { sentCol: 'emails_sent', maxCol: 'max_emails' };
    if (channel === 'linkedin_invitation') return { sentCol: 'linkedin_invitations_sent', maxCol: 'max_linkedin_invitations' };
    return { sentCol: 'linkedin_messages_sent', maxCol: 'max_linkedin_messages' };
  })();

  const { data, error } = await supabase
    .from('account_daily_limits')
    .select(`${sentCol}, ${maxCol}`)
    .eq('account_id', accountId)
    .eq('date', today)
    .maybeSingle();

  if (error) {
    console.error(`[preflight] account_daily_limits read failed acct=${accountId}: ${error.message}`);
    return false; // fail-open
  }

  const row = data as unknown as Record<string, number | null> | null;
  const sent = row ? Number(row[sentCol] ?? 0) : 0;
  const max = row ? Number(row[maxCol] ?? 0) : 0;

  capCache.set(cacheKey, { sent, max, expiresAt: now + CAP_CACHE_TTL_MS });
  return max > 0 && sent >= max;
}

/**
 * Per-sequence in-process lock. Realtime can deliver multiple events for the
 * same sequence in rapid succession (e.g., a batch INSERT of 60 executions
 * fires 60 events). Without serialization, each event spawns a parallel
 * handleWakeEvent that:
 *   - reads slotsUsed at wake-start (all see same stale value)
 *   - dispatches the same execution rows in parallel
 *   - pullNewLeadsForToday runs in parallel and over-pulls
 *
 * This Set holds the sequence_ids currently being processed. New wakes for
 * a held sequence return immediately — the in-flight processor will pick up
 * any new state via fetchDueExecutions, which queries fresh on each pass.
 */
const inFlightSequences = new Set<string>();

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

/** Max executions considered per wake event. Per-sequence, so small budget. */
const MAX_CANDIDATES_PER_WAKE = 500;

/**
 * Max *actual* dispatches a single wake emits per sequence. Skip decisions
 * (cap_reached, slot full, race) don't count. Tunes the trade-off between
 * fairness (more wakes = better interleaving across sequences sharing an
 * account) and orchestrator wake frequency. ~10 = ~35 sec of queue depth
 * at 17 emails/min worker pacing, which is short enough for a second
 * sequence on the same account to interleave its batch within a minute.
 */
const MAX_DISPATCHES_PER_WAKE = 10;

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
 *
 * Serialized per sequence_id: if another wake for this sequence is already
 * in flight, the new wake returns immediately. The in-flight processor will
 * re-query fresh state at the top of its loop, so no work is missed; we just
 * avoid parallel dispatchers double-claiming slots and double-pulling leads.
 */
export async function handleWakeEvent(event: OrchSequenceWakeEvent): Promise<void> {
  // Per-sequence serialization guard.
  if (inFlightSequences.has(event.sequenceId)) {
    return;
  }
  inFlightSequences.add(event.sequenceId);
  try {
    await _handleWakeEventInner(event);
  } finally {
    inFlightSequences.delete(event.sequenceId);
  }
}

async function _handleWakeEventInner(event: OrchSequenceWakeEvent): Promise<void> {
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

  // If nothing's due, skip the cohort loop but STILL run the new-lead pull
  // below — a fresh sequence with zero executions needs the pull to bootstrap
  // its first day. (Earlier version had an early return here that broke that.)

  // 6. Iterate candidates in cohort order. For each, decide slot need,
  //    claim if necessary, emit decision.
  //
  // Dispatch budget: cap how many *actual* dispatches this wake emits.
  // Without this, a sequence with 1000 due candidates would push 1000 jobs
  // into the per-account BullMQ queue in one wake — when multiple sequences
  // share an account, whoever woke first monopolises the FIFO queue and
  // the others starve.
  //
  // Capping per wake forces dispatches to chunk over time: each step_result
  // insert fires a Realtime UPDATE → the orchestrator wakes the sequence
  // again → another batch of `MAX_DISPATCHES_PER_WAKE` jobs is emitted.
  // Multiple sequences sharing one account each contribute a small batch
  // per wake, so their jobs interleave in the queue and both make
  // incremental progress.
  //
  // Skips (cap-reached, daily-limit-reached, slot-race) do NOT count
  // against the budget — they're cheap shadow-log entries with no queue
  // impact.
  let decisionsRecorded = 0;
  let dispatchesThisWake = 0;
  let dailyLimitReached = false;
  let dispatchBudgetExhausted = false;

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

    // Slot semantics: daily_batch_size caps FRESH lead starts per day only.
    // Multi-day follow-ups (P3) and same-day chain continuations (P2) are
    // leads-already-in-motion; they don't count against the daily quota
    // (per-account daily caps still bound total sends — that's the worker's
    // RPC, separate concern).
    //
    //   First touch          → P1 → claim slot
    //   Same-day continuation → P2 → first_touch_done already true, free
    //   Multi-day follow-up   → P3 → first_touch_done already true, free
    //
    // `hasSlot` still acts as the race guard for concurrent first-touch
    // wakes on the same contact.
    const isFirstTouch = !exec.first_touch_done;
    let consumesSlot = false;
    if (isFirstTouch) {
      try {
        consumesSlot = !(await hasSlot(seq.id, subjectId, localDate));
      } catch {
        // Slot read failed — fail closed by skipping this candidate this pass.
        continue;
      }
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

    // Pre-flight account-cap check (orchestrator-mode dispatch only).
    // Skips obviously-going-to-fail sends without round-tripping the worker.
    // The worker's atomic `check_and_increment_daily_limit` is still the
    // source of truth at send time; this is purely an optimization that
    // also keeps the orchestrator's view of "what we tried today" honest.
    // We do NOT release any slot we just claimed — the campaign committed
    // to this lead as today's first-touch; we just defer it until the cap
    // frees up (handled by the worker's +jitter reschedule on the next try
    // if dispatch does happen, or by the next wake if pre-flight skipped).
    if (mode === 'orchestrator') {
      const rawStep = exec.unipile_sequence_steps;
      const stepData = Array.isArray(rawStep) ? rawStep[0] : rawStep;
      const channel = capChannelForStep(stepData?.step_type ?? null);
      const channelAccount =
        channel === 'email'
          ? exec.assigned_email_account_id
          : channel === 'linkedin_invitation' || channel === 'linkedin_message'
            ? exec.assigned_linkedin_account_id
            : null;
      if (channel && channelAccount && (await accountAtCap(channelAccount, channel))) {
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
            skipReason: 'account_cap_reached',
          },
          event,
          mode,
          exec,
        );
        decisionsRecorded++;
        continue;
      }

      // Per-(sequence, account) in-flight cap.
      //
      // The per-wake budget alone isn't sufficient when one campaign has
      // a huge head start: its completed sends fire Realtime events at
      // ~worker-drain-rate, each producing a wake that re-fills its 10-job
      // chunk, dwarfing a freshly-started campaign's lone wakes. The
      // queue depth becomes ~95% the head-start campaign's jobs and the
      // worker drains FIFO accordingly. The newcomer never gets a fair
      // share.
      //
      // Capping in-flight per (sequence, account) bounds queue depth per
      // sequence regardless of wake rate. All sequences sharing an
      // account compete for the same fixed pool of slots; the orchestrator
      // can't dispatch sequence A's 31st job until sequence A's 1st job
      // completes (which the worker will do at its natural pace, mixing
      // it with whatever B has queued).
      if (channel && channelAccount) {
        const reserved = await tryReserveDispatchSlot(channelAccount, seq.id);
        if (!reserved) {
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
              skipReason: 'inflight_budget_full',
            },
            event,
            mode,
            exec,
          );
          decisionsRecorded++;
          continue;
        }
      }
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
    dispatchesThisWake++;

    // Stop after the budget is spent. Remaining candidates stay in
    // `running, not_started` with their existing next_execution_at — the
    // next Realtime wake (triggered by any of this wake's dispatches
    // completing and writing a step_result) picks them up. Multiple
    // sequences sharing an account take turns at this granularity.
    if (dispatchesThisWake >= MAX_DISPATCHES_PER_WAKE) {
      dispatchBudgetExhausted = true;
      break;
    }
  }

  console.log(
    `[scheduler] sequence=${seq.id} mode=${mode} decisions=${decisionsRecorded} ` +
      `dispatched=${dispatchesThisWake}` +
      (dailyLimitReached ? ' (daily_limit_reached during pass)' : '') +
      (dispatchBudgetExhausted ? ' (dispatch_budget_exhausted — waiting for next wake)' : ''),
  );

  // 7. Capacity-based new-lead pull. Replaces pre-batching entirely:
  //    look at how many fresh-start slots remain for today
  //    (daily_batch_size minus first-touches already started today), then
  //    pull exactly that many contacts from the sequence's lead pool that
  //    don't yet have an execution. INSERT them as first-touch executions.
  //
  //    This is what makes daily release self-throttling — drift can't
  //    compound because the pull is gated by actual slot consumption, not
  //    by a calendar-scheduled batch. Same-day chains and multi-day
  //    follow-ups dispatch normally above this step (they don't consume
  //    slots, per the corrected slot semantics).
  //
  //    Shadow mode never pulls — creating real executions would defeat
  //    "observe only."
  if (mode === 'orchestrator') {
    // Backfill safety net: release slots for leads that went terminal
    // (completed/failed) without ever making a send attempt — e.g., a
    // conditional-check skip ("already connected on LinkedIn"), missing
    // email, enrichment failure. Without this, those slots stay claimed
    // and the day's batch under-delivers.
    const released = await reconcileSoftFailedSlots(seq, localDate);
    let slotsAvailableForPull = slotsAvailable;
    if (released > 0) {
      const slotsUsedAfter = await countSlotsForDate(seq.id, localDate);
      slotsAvailableForPull = Math.max(0, dailyBudget - slotsUsedAfter);
      console.log(
        `[scheduler] sequence=${seq.id} reconciled ${released} soft-failed slots ` +
          `→ slotsAvailable ${slotsAvailable}→${slotsAvailableForPull}`,
      );
    }
    await pullNewLeadsForToday(seq, slotsAvailableForPull, localDate);
  }
}

/**
 * Release slots that were claimed for leads which subsequently went terminal
 * (status='completed' or 'failed') without the worker ever attempting a send.
 *
 * "Attempted a send" is defined as a row existing in `unipile_step_results`
 * for the execution. The worker writes that row when it calls the provider
 * (Unipile), regardless of outcome — success, 4xx, 5xx, bounce all leave a
 * trail. So "no row" = "we never tried."
 *
 * This intentionally handles only soft pre-send failures:
 *
 *   Released (slot freed, backfill kicks in):
 *     - Conditional-check skip ("already connected on LinkedIn" → completed)
 *     - Lead has no email / no LinkedIn URL (worker marks failed before send)
 *     - Enrichment / lookup failure before the provider call
 *
 *   NOT released (slot stays claimed):
 *     - Provider 4xx/5xx after send attempt → step_results row exists
 *     - Bounce after delivery → step_results row exists
 *     - Daily account cap hit → execution stays `running`, not terminal
 *     - Multi-day follow-ups → first_touch_done=true, never claimed a slot
 *
 * Cheap: three small queries gated by today's slot set (capped at
 * daily_batch_size, typically dozens to a few hundred rows).
 */
async function reconcileSoftFailedSlots(
  seq: SequenceMeta,
  localDate: string,
): Promise<number> {
  // 1. Today's claimed slots for this sequence.
  const { data: slots, error: slotErr } = await supabase
    .from('unipile_sequence_daily_leads')
    .select('contact_id')
    .eq('unipile_sequence_id', seq.id)
    .eq('date', localDate);
  if (slotErr) {
    console.error(`[reconcile] slot read failed sequence=${seq.id}: ${slotErr.message}`);
    return 0;
  }
  if (!slots || slots.length === 0) return 0;

  const contactIds = (slots as Array<{ contact_id: string }>).map((s) => s.contact_id);

  // 2. Terminal executions on these contacts that never first-touched.
  //    Anything still `running` is by definition not a soft failure.
  const { data: execs, error: execErr } = await supabase
    .from('unipile_sequence_executions')
    .select('id, contact_id')
    .eq('unipile_sequence_id', seq.id)
    .in('contact_id', contactIds)
    .in('status', ['completed', 'failed'])
    .eq('first_touch_done', false);
  if (execErr) {
    console.error(`[reconcile] exec read failed sequence=${seq.id}: ${execErr.message}`);
    return 0;
  }
  if (!execs || execs.length === 0) return 0;

  const execRows = execs as Array<{ id: string; contact_id: string }>;
  const execIds = execRows.map((e) => e.id);

  // 3. Step-result existence check. Only executions with ZERO step_results
  //    are eligible for slot release — anything else means the worker
  //    actually called the provider and consumed real send budget.
  const { data: attempts, error: attemptsErr } = await supabase
    .from('unipile_step_results')
    .select('execution_id')
    .in('execution_id', execIds);
  if (attemptsErr) {
    console.error(
      `[reconcile] step_results read failed sequence=${seq.id}: ${attemptsErr.message}`,
    );
    return 0;
  }
  const attemptedExecIds = new Set(
    (attempts ?? []).map((a) => (a as { execution_id: string }).execution_id),
  );

  const contactsToRelease = execRows
    .filter((e) => !attemptedExecIds.has(e.id))
    .map((e) => e.contact_id);

  if (contactsToRelease.length === 0) return 0;

  // 4. Delete the slot rows. The contacts stay in `unipile_sequence_executions`
  //    in their terminal state — we just stop counting them against today's
  //    fresh-start quota so the pull can backfill with replacement contacts.
  const { error: delErr } = await supabase
    .from('unipile_sequence_daily_leads')
    .delete()
    .eq('unipile_sequence_id', seq.id)
    .eq('date', localDate)
    .in('contact_id', contactsToRelease);
  if (delErr) {
    console.error(`[reconcile] slot release failed sequence=${seq.id}: ${delErr.message}`);
    return 0;
  }
  return contactsToRelease.length;
}

/**
 * Pull new contacts from the sequence's source list and materialize them
 * as first-touch executions, capped at `slotsAvailable`.
 *
 * Source detection: if the sequence has a `contact_list_id` use the
 * `contacts` table; otherwise fall back to `lead_list_id` → `leads`.
 *
 * Account assignment: round-robin from the sequence's rotating account
 * pools (`unipile_sequence_email_accounts`, `unipile_sequence_linkedin_accounts`)
 * with fallback to `sequence.configuration.selectedEmailAccountId` /
 * `selectedLinkedInAccountId`. Matches `unipile-process-batch-queue`'s
 * pattern exactly so the worker's existing send path works unchanged.
 *
 * Debouncer reuses the same `lastMaterializeAt` map keyed per-sequence so
 * back-to-back Realtime events don't issue parallel pulls for the same
 * sequence.
 */
async function pullNewLeadsForToday(
  seq: SequenceMeta,
  slotsAvailable: number,
  localDate: string,
): Promise<void> {
  if (slotsAvailable <= 0) return;

  // Per-sequence debouncer — different key from the legacy batch
  // materializer's client-level one to avoid cross-contamination.
  // Set the cooldown timestamp BEFORE any async work so parallel calls
  // can't all sail past the check together (the original race that
  // caused over-pulling in the 60→177 incident). The in-flight wake lock
  // above is now the primary protection, but defense in depth.
  const cooldownKey = `pull:${seq.id}`;
  const now = Date.now();
  const last = lastMaterializeAt.get(cooldownKey) ?? 0;
  if (now - last < MATERIALIZE_COOLDOWN_MS) return;
  lastMaterializeAt.set(cooldownKey, now);

  // Decide source: contact-based vs lead-based sequence.
  const isContactBased = !!seq.contact_list_id;
  const sourceListId = isContactBased ? seq.contact_list_id : seq.lead_list_id;
  if (!sourceListId) return; // no source list — sequence misconfigured

  const idColumn: 'contact_id' | 'lead_id' = isContactBased ? 'contact_id' : 'lead_id';

  // 1. Fetch first step of the sequence.
  const { data: firstStepData } = await supabase
    .from('unipile_sequence_steps')
    .select('id')
    .eq('unipile_sequence_id', seq.id)
    .order('step_order', { ascending: true })
    .limit(1)
    .maybeSingle();
  if (!firstStepData) return;
  const firstStepId = (firstStepData as { id: string }).id;

  // 2. Pull unassigned candidates via Postgres RPC.
  //    The RPC does the anti-join (`NOT EXISTS` against
  //    unipile_sequence_executions) server-side, which keeps the query
  //    O(log n) on existing-execution count via `idx_executions_contact_id` /
  //    `idx_executions_lead_id`. The previous client-side approach (fetch all
  //    existing IDs then send them back as a `NOT IN (...)` URL literal)
  //    failed at ~5k+ leads per sequence due to PostgREST URL limits.
  const rpcName = isContactBased ? 'orch_pull_unassigned_contacts' : 'orch_pull_unassigned_leads';
  const rpcArgs: Record<string, unknown> = {
    p_sequence_id: seq.id,
    p_limit: slotsAvailable,
  };
  if (isContactBased) {
    rpcArgs.p_contact_list_id = sourceListId;
  } else {
    rpcArgs.p_lead_list_id = sourceListId;
  }

  const { data: newContacts, error: pullErr } = await supabase.rpc(rpcName, rpcArgs);
  if (pullErr) {
    console.error(`[pull] RPC ${rpcName} failed for sequence=${seq.id}:`, pullErr.message);
    return;
  }
  if (!newContacts || newContacts.length === 0) return;

  // 4. Resolve rotating accounts for assignment.
  const [emailAccounts, linkedInAccounts] = await Promise.all([
    fetchRotatingAccounts(seq.id, 'email'),
    fetchRotatingAccounts(seq.id, 'linkedin'),
  ]);
  const cfg = seq.configuration ?? {};
  const fallbackEmail = cfg.selectedEmailAccountId ?? null;
  const fallbackLinkedIn = cfg.selectedLinkedInAccountId ?? null;

  // 5. Build execution rows. Match the shape produced by
  //    unipile-process-batch-queue so the worker's existing send path
  //    works unchanged.
  const startedAtIso = new Date().toISOString();
  const executions = newContacts.map((c, i) => {
    const jitterSeconds = Math.floor(Math.random() * 60);
    const nextExec = new Date(Date.now() + jitterSeconds * 1000).toISOString();
    const assignedEmail =
      emailAccounts.length > 0
        ? emailAccounts[i % emailAccounts.length]
        : fallbackEmail;
    const assignedLinkedIn =
      linkedInAccounts.length > 0
        ? linkedInAccounts[i % linkedInAccounts.length]
        : fallbackLinkedIn;
    return {
      unipile_sequence_id: seq.id,
      [idColumn]: (c as { id: string }).id,
      current_step_id: firstStepId,
      status: 'running' as const,
      started_at: startedAtIso,
      next_execution_at: nextExec,
      execution_state: 'not_started' as const,
      priority_cohort: 'new_today' as const,
      first_touch_done: false,
      assigned_email_account_id: assignedEmail,
      assigned_linkedin_account_id: assignedLinkedIn,
    };
  });

  // 6. Insert executions + claim slots in parallel.
  const { error: insertErr } = await supabase
    .from('unipile_sequence_executions')
    .insert(executions);
  if (insertErr) {
    // Bulk insert is all-or-nothing — log and bail. The cooldown will
    // back off the retry naturally; partial unique indexes catch any race.
    console.error(`[pull] execution insert failed for sequence=${seq.id}:`, insertErr.message);
    return;
  }

  // Claim slots (best-effort — slot rows are tracking metadata, not
  // gate-keepers at this point; insert above already committed).
  const slotRows = newContacts.map((c) => ({
    unipile_sequence_id: seq.id,
    contact_id: (c as { id: string }).id, // slot table now keys on contact_id
    date: localDate,
  }));
  await supabase
    .from('unipile_sequence_daily_leads')
    .upsert(slotRows, {
      onConflict: 'unipile_sequence_id,contact_id,date',
      ignoreDuplicates: true,
    });

  // Cooldown was already set at function entry. Just log the result.
  console.log(
    `[pull] sequence=${seq.id} pulled ${newContacts.length} new contacts ` +
      `(slotsAvailable was ${slotsAvailable}, source=${isContactBased ? 'contacts' : 'leads'})`,
  );
}

/**
 * Fetch the rotating account pool for a sequence. Returns the
 * `unipile_account_id` strings ordered by `priority_order`.
 */
async function fetchRotatingAccounts(
  sequenceId: string,
  channel: 'email' | 'linkedin',
): Promise<string[]> {
  const tableName =
    channel === 'email'
      ? 'unipile_sequence_email_accounts'
      : 'unipile_sequence_linkedin_accounts';
  const { data, error } = await supabase
    .from(tableName)
    .select('unipile_account_id, priority_order')
    .eq('unipile_sequence_id', sequenceId)
    .eq('is_active', true)
    .order('priority_order', { ascending: true });
  if (error || !data) return [];
  return data
    .map((r) => (r as { unipile_account_id: string }).unipile_account_id)
    .filter(Boolean);
}

// (materializeDueBatches removed — replaced by capacity-based pullNewLeadsForToday)

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
  contact_list_id: string | null;
  lead_list_id: string | null;
  configuration: { selectedEmailAccountId?: string; selectedLinkedInAccountId?: string } | null;
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
        'scheduled_start_time, scheduled_end_time, active_days, ' +
        'contact_list_id, lead_list_id, configuration',
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
  candidate: ExecutionCandidate,
): Promise<void> {
  // Fresh nonce per dispatch — prevents BullMQ jobId dedup from silently
  // dropping legitimate re-enqueues (e.g., when an execution was previously
  // refused by the daily-cap RPC, "completed" the BullMQ job without sending,
  // and is now eligible again after the counter rolled over or an admin
  // manually rescheduled it).
  //
  // Worker's stale-job check on `unipile_step_results` is the actual
  // duplicate-send protection — it runs BEFORE the Unipile call regardless
  // of jobId. So jobId uniqueness here is purely about queue admittance,
  // not send safety.
  const dispatchNonce = randomBytes(3).toString('hex');
  const jobId = `exec-${decision.executionId}-${decision.stepId}-${ORCH_SESSION_ID}-${dispatchNonce}`;

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
    // BullMQ add failed — the in-flight slot was reserved upstream in the
    // cohort loop but no worker will ever release it (no job to release
    // from). Roll back the reservation so the counter stays accurate.
    // Mirrors the channelAccount derivation from the cohort loop's
    // pre-flight cap check.
    const rawStep = candidate.unipile_sequence_steps;
    const stepData = Array.isArray(rawStep) ? rawStep[0] : rawStep;
    const channel = capChannelForStep(stepData?.step_type ?? null);
    const channelAccount =
      channel === 'email'
        ? candidate.assigned_email_account_id
        : channel === 'linkedin_invitation' || channel === 'linkedin_message'
          ? candidate.assigned_linkedin_account_id
          : null;
    if (channelAccount) {
      try {
        await releaseDispatchSlot(channelAccount, decision.sequenceId);
      } catch {
        // best-effort rollback; the 24h TTL on the counter key is the
        // last-line safety net if this also fails
      }
    }
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
