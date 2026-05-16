/**
 * Shadow-mode decision logger.
 *
 * In `shadow` mode, every decision the orchestrator *would* have acted on is
 * written here. We never enqueue in shadow. Comparison happens out-of-band
 * via SQL (see STAGING.md → "shadow gates").
 */

import { supabase } from '../supabase';
import type { OrchDecision, OrchSequenceWakeEvent, ShadowLogRow } from './types';

export async function recordShadowDecision(
  decision: OrchDecision,
  triggeringEvent: OrchSequenceWakeEvent,
): Promise<void> {
  const row: ShadowLogRow = {
    sequence_id: decision.sequenceId,
    client_id: decision.clientId,
    lead_id: decision.leadId,
    execution_id: decision.executionId,
    step_id: decision.stepId,
    cohort_label: decision.cohortLabel,
    cohort_priority: decision.cohortPriority,
    intended_enqueue_at: decision.nextExecutionAt,
    consumes_slot: decision.consumesSlot,
    skip_reason: decision.skipReason ?? null,
    observed_event_source: triggeringEvent.source,
    recorded_at: new Date().toISOString(),
  };

  const { error } = await supabase.from('orchestrator_shadow_log').insert(row);
  if (error) {
    // Shadow log writes must never break the dispatch path. Log and move on;
    // the same comparison query that gates promotion will surface the gap.
    console.error(`[shadow] insert failed for sequence=${decision.sequenceId}:`, error.message);
  }
}

/**
 * Batched variant for high-throughput shadow runs. Same semantics — never
 * throws. Caller is responsible for chunking to <= 1000 rows per call.
 */
export async function recordShadowDecisionsBatch(
  decisions: Array<{ decision: OrchDecision; event: OrchSequenceWakeEvent }>,
): Promise<void> {
  if (decisions.length === 0) return;
  const rows: ShadowLogRow[] = decisions.map(({ decision, event }) => ({
    sequence_id: decision.sequenceId,
    client_id: decision.clientId,
    lead_id: decision.leadId,
    execution_id: decision.executionId,
    step_id: decision.stepId,
    cohort_label: decision.cohortLabel,
    cohort_priority: decision.cohortPriority,
    intended_enqueue_at: decision.nextExecutionAt,
    consumes_slot: decision.consumesSlot,
    skip_reason: decision.skipReason ?? null,
    observed_event_source: event.source,
    recorded_at: new Date().toISOString(),
  }));

  const { error } = await supabase.from('orchestrator_shadow_log').insert(rows);
  if (error) {
    console.error(`[shadow] batch insert (${rows.length} rows) failed:`, error.message);
  }
}
