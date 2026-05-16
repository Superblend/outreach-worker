/**
 * Reads `clients.orchestrator_mode` with a short TTL cache.
 *
 * Hot path: scheduler calls this once per (sequence, decision pass) to
 * confirm the client hasn't been flipped to legacy mid-flight. A 30-second
 * cache is enough to absorb burst queries without making rollback slow.
 */

import { supabase } from '../supabase';
import type { OrchestratorMode } from './types';

interface CacheEntry {
  mode: OrchestratorMode;
  fetchedAt: number;
}

const CACHE_TTL_MS = 30_000;
const cache = new Map<string, CacheEntry>();

/**
 * Returns the current `orchestrator_mode` for a client, or `'legacy'` if the
 * client row is missing (defensive default — never schedule for a client we
 * can't identify).
 */
export async function getOrchestratorMode(clientId: string): Promise<OrchestratorMode> {
  const cached = cache.get(clientId);
  if (cached && Date.now() - cached.fetchedAt < CACHE_TTL_MS) {
    return cached.mode;
  }

  const { data, error } = await supabase
    .from('clients')
    .select('orchestrator_mode')
    .eq('id', clientId)
    .maybeSingle();

  if (error || !data) {
    // Cache the defensive default briefly so we don't hammer the DB on a
    // missing client_id. A row that genuinely doesn't exist won't appear in a
    // realtime event we care about anyway.
    cache.set(clientId, { mode: 'legacy', fetchedAt: Date.now() });
    return 'legacy';
  }

  const mode = (data.orchestrator_mode ?? 'legacy') as OrchestratorMode;
  cache.set(clientId, { mode, fetchedAt: Date.now() });
  return mode;
}

/** Explicit invalidation — called after a known mode change (e.g., admin UI). */
export function invalidateMode(clientId: string): void {
  cache.delete(clientId);
}

/** Diagnostics — current cache state, for `/health` JSON. */
export function modeCacheStats(): { size: number; entries: number } {
  return { size: cache.size, entries: cache.size };
}
