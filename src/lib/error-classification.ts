// Pure predicates over provider error strings. Kept out of execution.worker.ts
// so they can be unit-tested without pulling in config/Redis/Supabase.

/**
 * The upstream mailbox/social provider is temporarily unreachable from Unipile.
 *
 * Unipile answers `422 errors/provider_unreachable — "Provider unreachable, try
 * again later."` for this. Despite the 4xx it is explicitly NOT a rejection of
 * our request: the account still resolves and folders still list, only the
 * provider-backed calls fail. In practice it means the OAuth grant has gone
 * stale and the mailbox needs reconnecting.
 *
 * It must never burn the lead. Before this existed, every one of these
 * completed the execution as `<step>_failed` on the first attempt — which is
 * how one client lost 100+ leads to an 11-day Google outage without a single
 * email being sent.
 */
export function isProviderUnavailableError(message: string): boolean {
  const lower = (message || '').toLowerCase();
  return (
    lower.includes('provider_unreachable') ||
    lower.includes('provider unreachable') ||
    lower.includes('try again later')
  );
}