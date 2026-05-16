/**
 * 5-minute poll fallback.
 *
 * Runs on a fixed cadence regardless of Realtime health. Two jobs:
 *   1. Catch anything Realtime missed (dropped events, late notifications).
 *   2. Be the *only* trigger when Realtime is offline entirely.
 *
 * The poll fires the same `onWake` handler as the Realtime subscriber but
 * with `source: 'poll-fallback'` so the scheduler can log/track which path
 * surfaced each decision. Shadow-mode comparison tables this distinction.
 */

import { supabase } from '../supabase';
import type { OrchSequenceWakeEvent } from './types';

type WakeHandler = (event: OrchSequenceWakeEvent) => Promise<void> | void;

const POLL_INTERVAL_MS = 5 * 60_000;

let pollTimer: NodeJS.Timeout | null = null;

export function startPollFallback(onWake: WakeHandler): void {
  if (pollTimer) return; // idempotent
  pollTimer = setInterval(() => runPollCycle(onWake), POLL_INTERVAL_MS);
  // Run once on startup so we don't wait 5 minutes for the first cycle.
  setTimeout(() => runPollCycle(onWake), 5_000);
  console.log(`[poll] fallback started (interval ${POLL_INTERVAL_MS / 1000}s)`);
}

export function stopPollFallback(): void {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

async function runPollCycle(onWake: WakeHandler): Promise<void> {
  try {
    // Two-step query — PostgREST can't auto-resolve a deep
    // executions → sequences → clients nested embed, so we do the client
    // filter as a separate lookup. Both steps are individually cheap and
    // well-indexed.
    //
    // Step 1: which clients are in shadow or orchestrator mode?
    const { data: eligibleClients, error: clientsErr } = await supabase
      .from('clients')
      .select('id')
      .in('orchestrator_mode', ['shadow', 'orchestrator']);

    if (clientsErr) {
      console.error('[poll] eligible-clients lookup failed:', clientsErr.message);
      return;
    }
    if (!eligibleClients || eligibleClients.length === 0) {
      // Nothing to do — every client is on legacy mode.
      return;
    }
    const clientIds = eligibleClients.map((c) => c.id);

    // Step 2: due executions for sequences owned by those clients.
    const fifteenMinAgo = new Date(Date.now() - 15 * 60_000).toISOString();
    const nowIso = new Date().toISOString();

    const { data, error } = await supabase
      .from('unipile_sequence_executions')
      .select(`
        unipile_sequence_id,
        unipile_sequences!inner(client_id, status, use_bullmq)
      `)
      .eq('status', 'running')
      .eq('unipile_sequences.status', 'active')
      .eq('unipile_sequences.use_bullmq', true)
      .in('unipile_sequences.client_id', clientIds)
      .lte('next_execution_at', nowIso)
      .lt('updated_at', fifteenMinAgo)
      .limit(2000);

    if (error) {
      console.error('[poll] cycle query failed:', error.message);
      return;
    }
    if (!data || data.length === 0) return;

    // Dedupe per sequence — one wake per sequence per cycle.
    const seen = new Set<string>();
    for (const row of data) {
      const seqId = row.unipile_sequence_id;
      if (!seqId || seen.has(seqId)) continue;
      seen.add(seqId);

      const sequence = (row as unknown as { unipile_sequences?: { client_id?: string } }).unipile_sequences;
      const clientId = sequence?.client_id ?? '';

      const event: OrchSequenceWakeEvent = {
        source: 'poll-fallback',
        sequenceId: seqId,
        clientId,
        observedAt: new Date().toISOString(),
      };
      try {
        await onWake(event);
      } catch (err) {
        console.error(`[poll] handler error for sequence=${seqId}:`, err);
      }
    }

    console.log(`[poll] fallback cycle: ${seen.size} sequences woken`);
  } catch (err) {
    console.error('[poll] cycle crashed:', err);
  }
}
