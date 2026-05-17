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
    // Three-step query — PostgREST can't auto-resolve a deep
    // executions → sequences → clients nested embed, so we do the client
    // filter as a separate lookup. Each step is individually cheap and
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

    // Dedupe per sequence across both phases — one wake per sequence per cycle.
    const seen = new Set<string>();
    const wakeSequence = async (seqId: string, clientId: string): Promise<void> => {
      if (!seqId || seen.has(seqId)) return;
      seen.add(seqId);
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
    };

    // Step 2: due executions for sequences owned by those clients.
    //   Catches sequences with overdue work that Realtime missed.
    const fifteenMinAgo = new Date(Date.now() - 15 * 60_000).toISOString();
    const nowIso = new Date().toISOString();

    const { data: dueRows, error: dueErr } = await supabase
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

    if (dueErr) {
      console.error('[poll] due-executions query failed:', dueErr.message);
    } else if (dueRows) {
      for (const row of dueRows) {
        const sequence = (row as unknown as { unipile_sequences?: { client_id?: string } }).unipile_sequences;
        await wakeSequence(row.unipile_sequence_id, sequence?.client_id ?? '');
      }
    }
    const dueWokenCount = seen.size;

    // Step 3: every active orchestrator-mode sequence, regardless of whether
    // anything is due right now. This closes the gap where new contacts get
    // added to an existing source list mid-day on an otherwise-idle sequence
    // — `contacts` isn't in our Realtime subscription, so without this nudge
    // the new contact wouldn't be pulled until the next bit of activity. The
    // wake handler's window check + 30s pull cooldown make these cheap for
    // sequences that have nothing new to do.
    const { data: activeSeqs, error: seqsErr } = await supabase
      .from('unipile_sequences')
      .select('id, client_id')
      .eq('status', 'active')
      .eq('use_bullmq', true)
      .in('client_id', clientIds);

    if (seqsErr) {
      console.error('[poll] active-sequences lookup failed:', seqsErr.message);
    } else if (activeSeqs) {
      for (const seq of activeSeqs) {
        await wakeSequence(seq.id, seq.client_id);
      }
    }
    const capacityWokenCount = seen.size - dueWokenCount;

    if (seen.size > 0) {
      console.log(
        `[poll] fallback cycle: ${seen.size} sequences woken ` +
          `(due=${dueWokenCount}, capacity-sweep=${capacityWokenCount})`,
      );
    }
  } catch (err) {
    console.error('[poll] cycle crashed:', err);
  }
}
