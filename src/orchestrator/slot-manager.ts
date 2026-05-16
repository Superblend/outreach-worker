/**
 * `unipile_sequence_daily_leads` slot reservation.
 *
 * Semantics — full details in README.md, condensed here:
 *   - Slot is per (sequence, lead, local_date_in_sequence_tz).
 *   - Existence of a row means "this lead has been started today for this seq".
 *   - Inserts use ON CONFLICT DO NOTHING so two orchestrator passes can't
 *     double-claim. The pre-check + insert pair is atomic.
 *   - Failed executions do NOT release the slot (matches today's behavior).
 *   - Retention 30 days, via daily housekeeping function (see migration).
 *
 * IMPORTANT: This file MUST NOT call `check_and_increment_daily_limit`.
 * That RPC governs per-Unipile-account caps and is the worker's concern.
 * Slot manager only governs per-sequence `daily_batch_size` budgeting.
 */

import { supabase } from '../supabase';
import { localDateString } from '../lib/time-utils';

/** Returns YYYY-MM-DD for "today" in the sequence's local timezone. */
export function todayInSequenceTz(timezone: string, now: Date = new Date()): string {
  return localDateString(now, timezone || 'UTC');
}

/**
 * How many distinct leads have a slot row for this sequence on this date.
 * This is the *only* gate on per-sequence daily_batch_size.
 */
export async function countSlotsForDate(
  sequenceId: string,
  localDate: string,
): Promise<number> {
  const { count, error } = await supabase
    .from('unipile_sequence_daily_leads')
    .select('lead_id', { count: 'exact', head: true })
    .eq('unipile_sequence_id', sequenceId)
    .eq('date', localDate);

  if (error) {
    // Fail open is dangerous here — we'd over-schedule. Fail closed: assume
    // slots are saturated and skip this pass. Next wake-up retries.
    console.error(`[slot] count failed for sequence=${sequenceId} date=${localDate}:`, error.message);
    throw new Error(`slot count query failed: ${error.message}`);
  }
  return count ?? 0;
}

/**
 * Does this lead already have a slot for today in this sequence's local tz?
 * Used to short-circuit same-day chain continuations (P2) — no slot needed.
 */
export async function hasSlot(
  sequenceId: string,
  leadId: string,
  localDate: string,
): Promise<boolean> {
  const { data, error } = await supabase
    .from('unipile_sequence_daily_leads')
    .select('lead_id')
    .eq('unipile_sequence_id', sequenceId)
    .eq('lead_id', leadId)
    .eq('date', localDate)
    .maybeSingle();

  if (error) {
    console.error(`[slot] hasSlot failed for ${sequenceId}/${leadId}/${localDate}:`, error.message);
    throw new Error(`slot lookup failed: ${error.message}`);
  }
  return Boolean(data);
}

/**
 * Atomically claim a slot. Returns `true` if newly inserted, `false` if a row
 * already existed (e.g., a concurrent orchestrator pass beat us to it — also
 * a valid outcome).
 *
 * Composite PK (sequence_id, lead_id, date) gives us the dedupe for free.
 */
export async function claimSlot(
  sequenceId: string,
  leadId: string,
  localDate: string,
): Promise<boolean> {
  const { data, error } = await supabase
    .from('unipile_sequence_daily_leads')
    .upsert(
      {
        unipile_sequence_id: sequenceId,
        lead_id: leadId,
        date: localDate,
      },
      { onConflict: 'unipile_sequence_id,lead_id,date', ignoreDuplicates: true },
    )
    .select('lead_id');

  if (error) {
    console.error(`[slot] claim failed for ${sequenceId}/${leadId}/${localDate}:`, error.message);
    throw new Error(`slot claim failed: ${error.message}`);
  }
  // `data` is empty if the row already existed (because we asked for
  // ignoreDuplicates). New rows return with the inserted lead_id.
  return Array.isArray(data) && data.length > 0;
}
