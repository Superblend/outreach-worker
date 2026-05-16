/**
 * Supabase Realtime subscription — primary event source.
 *
 * Subscribes to changes on the tables that should trigger a per-sequence
 * scheduler pass:
 *   - `unipile_sequence_executions` INSERT (new lead added mid-day) or
 *      UPDATE of (status / next_execution_at / current_step_id)
 *   - `unipile_sequences` UPDATE of status (paused → active, etc.)
 *   - `unipile_step_results` INSERT (a send completed — possibly enables a
 *      same-day chain continuation)
 *
 * Reconnect strategy:
 *   - Realtime client auto-reconnects, but we also track `lastEventAt` and
 *     emit a `stale` event if no traffic for N minutes. Poll fallback uses
 *     this signal to step in.
 *
 * IMPLEMENTATION NOTE (review): we're using @supabase/supabase-js v2 channels.
 * Filter scope is per-table; we will receive ALL events project-wide. The
 * scheduler is responsible for filtering by `clients.orchestrator_mode` and
 * `unipile_sequences.use_bullmq` before acting. This keeps the subscription
 * topology simple — one channel per table, not per client.
 */

import { supabase } from '../supabase';
import type { OrchSequenceWakeEvent } from './types';

type WakeHandler = (event: OrchSequenceWakeEvent) => Promise<void> | void;

interface SubscriberStats {
  startedAt: number;
  lastEventAt: number;
  totalEvents: number;
  reconnectCount: number;
}

let stats: SubscriberStats | null = null;
let handler: WakeHandler | null = null;

export function startRealtimeSubscriber(onWake: WakeHandler): void {
  handler = onWake;
  stats = { startedAt: Date.now(), lastEventAt: Date.now(), totalEvents: 0, reconnectCount: 0 };

  // Channel 1 — execution lifecycle. Most events come from here.
  supabase
    .channel('orch-executions')
    .on(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      'postgres_changes' as any,
      { event: '*', schema: 'public', table: 'unipile_sequence_executions' },
      (payload) => onTableEvent(payload, 'unipile_sequence_executions'),
    )
    .subscribe((status) => {
      console.log(`[realtime] executions channel: ${status}`);
      if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT') {
        if (stats) stats.reconnectCount++;
      }
    });

  // Channel 2 — sequence-level changes (status flips, schedule edits).
  supabase
    .channel('orch-sequences')
    .on(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      'postgres_changes' as any,
      { event: 'UPDATE', schema: 'public', table: 'unipile_sequences' },
      (payload) => onTableEvent(payload, 'unipile_sequences'),
    )
    .subscribe();

  // Channel 3 — step result inserts. Auto-promotion trigger fires here, so a
  // result insert may have made the next step due immediately (same-day chain).
  supabase
    .channel('orch-step-results')
    .on(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      'postgres_changes' as any,
      { event: 'INSERT', schema: 'public', table: 'unipile_step_results' },
      (payload) => onTableEvent(payload, 'unipile_step_results'),
    )
    .subscribe();

  console.log('[realtime] subscriber started');
}

/**
 * Internal: normalize a Realtime payload into an `OrchSequenceWakeEvent`.
 * The scheduler does the actual decision work; we just route the wake.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function onTableEvent(payload: any, table: string): void {
  if (!stats || !handler) return;
  stats.lastEventAt = Date.now();
  stats.totalEvents++;

  // Extract sequence_id and client_id based on the table.
  let sequenceId: string | undefined;
  let clientId: string | undefined;

  const newRow = payload?.new ?? payload?.record ?? {};

  if (table === 'unipile_sequence_executions') {
    sequenceId = newRow.unipile_sequence_id;
    // client_id is on the sequence, not the execution — scheduler will resolve.
  } else if (table === 'unipile_sequences') {
    sequenceId = newRow.id;
    clientId = newRow.client_id;
  } else if (table === 'unipile_step_results') {
    sequenceId = newRow.unipile_sequence_id;
  }

  if (!sequenceId) return; // not actionable

  const event: OrchSequenceWakeEvent = {
    source: 'realtime',
    sequenceId,
    clientId: clientId ?? '', // scheduler will resolve from sequence row
    observedAt: new Date().toISOString(),
  };

  // Fire and forget — handler is responsible for its own error containment.
  Promise.resolve(handler(event)).catch((err) => {
    console.error('[realtime] handler error:', err);
  });
}

/** Diagnostics for `/health` endpoint and watchdog logic. */
export function subscriberStats(): SubscriberStats | null {
  return stats ? { ...stats } : null;
}

/** True if Realtime is "warm" — i.e., we've seen events recently. */
export function isRealtimeHealthy(staleThresholdMs = 10 * 60_000): boolean {
  if (!stats) return false;
  return Date.now() - stats.lastEventAt < staleThresholdMs;
}
