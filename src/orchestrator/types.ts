/**
 * Shared types for the orchestrator service.
 *
 * Naming convention: prefix internal-only types with `Orch` so they don't
 * collide with the worker's own types (some of which we'll import).
 */

/** Per-client opt-in flag stored in `clients.orchestrator_mode`. */
export type OrchestratorMode = 'legacy' | 'shadow' | 'orchestrator';

/** A "wake up and consider this sequence" event coming from Realtime or poll. */
export interface OrchSequenceWakeEvent {
  source: 'realtime' | 'poll-fallback' | 'initial-scan';
  sequenceId: string;
  clientId: string;
  /** ISO timestamp the event was observed (not necessarily when the row changed). */
  observedAt: string;
}

/**
 * The orchestrator's decision for a single (sequence, lead) pair after
 * cohort + slot + window analysis. In shadow mode this is written to
 * `orchestrator_shadow_log`. In `orchestrator` mode it drives an enqueue.
 */
export interface OrchDecision {
  sequenceId: string;
  clientId: string;
  /**
   * The contact this decision is about. `unipile_sequence_executions.contact_id`
   * is the current model for the message subject; `lead_id` on executions is
   * legacy and mostly NULL in recent data. Empty string allowed for skip-only
   * decisions that don't pertain to a specific contact.
   */
  contactId: string;
  /** Existing execution row, if any. Null means "create a new execution". */
  executionId: string | null;
  /** Step the orchestrator intends to run. */
  stepId: string;

  /** P1–P4 cohort tier as computed by `cohortPriority()` from scanner.ts. */
  cohortPriority: number;
  cohortLabel: OrchCohortLabel;

  /** When this should fire, ISO. May be in the future for delayed cases. */
  nextExecutionAt: string;

  /**
   * Whether this decision required a *new* slot in
   * `unipile_sequence_daily_leads` for today. False means "already had a slot
   * (same-day chain continuation)" — no slot was claimed.
   */
  consumesSlot: boolean;

  /** Why this was deferred or skipped, if applicable. */
  skipReason?: OrchSkipReason;
}

export type OrchCohortLabel =
  /** new lead, first touch not yet completed */
  | 'p1_new_contact'
  /** lead touched earlier today, same-day chain continuation due */
  | 'p2_same_day_chain'
  /** lead touched on a prior day, multi-day follow-up due */
  | 'p3_multi_day_follow_up'
  /** low priority bucket */
  | 'p4_low_priority';

export type OrchSkipReason =
  /** Sequence is outside its active window in local timezone */
  | 'outside_active_window'
  /** Today is not in `active_days` */
  | 'inactive_day'
  /** Daily slot budget exhausted for this sequence */
  | 'daily_limit_reached'
  /** Sequence status is not active */
  | 'sequence_not_active'
  /** Client's `orchestrator_mode` is `legacy` (we shouldn't even be here) */
  | 'client_legacy_mode'
  /** Sequence is the legacy `use_bullmq=false` path */
  | 'not_bullmq_sequence'
  /** Assigned account is at its per-day cap; will be retried on next wake */
  | 'account_cap_reached';

/** Row shape for `unipile_sequence_daily_leads` slot reservation table. */
export interface SlotRow {
  unipile_sequence_id: string;
  contact_id: string;
  /** Date in the sequence's local timezone, YYYY-MM-DD. */
  date: string;
  slot_claimed_at: string;
}

/** Row shape for `orchestrator_shadow_log` (shadow phase comparison table). */
export interface ShadowLogRow {
  sequence_id: string;
  client_id: string;
  contact_id: string;
  execution_id: string | null;
  step_id: string;
  cohort_label: OrchCohortLabel;
  cohort_priority: number;
  intended_enqueue_at: string;
  consumes_slot: boolean;
  skip_reason: OrchSkipReason | null;
  observed_event_source: OrchSequenceWakeEvent['source'];
  recorded_at: string;
}
