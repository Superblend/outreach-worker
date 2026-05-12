// Dual-read cutoff: pre-A4 executions wrote action:'completed' as the idempotency anchor.
// Remove LEGACY_LOG_CUTOFF and the OR branch in findSentLogEntry() after 2026-06-12.
export const LEGACY_LOG_CUTOFF = '2026-05-12T15:00:00.000Z';

/**
 * Returns the log entry proving a step was already sent, or undefined if none exists.
 *
 * Accepts two shapes:
 *   - action:'sent'  — new anchor written post-A4 deploy (no timestamp gate)
 *   - action:'completed' with ts/executed_at < cutoff — legacy anchor from pre-A4
 *     executions; the timestamp gate prevents post-cutoff 'completed' entries
 *     (written by unrelated advance logic) from blocking re-execution.
 *
 * String-lex comparison on ISO 8601 is correct because the format is fixed-width
 * and zero-padded. String() coercion before the comparison ensures that a numeric
 * epoch-ms value (e.g. 1747065600000) is compared as '174...' not coerced to NaN.
 * Fallback to '' for missing/null ts ensures a missing timestamp is treated as
 * pre-cutoff (skip rather than re-execute — safe default).
 */
export function findSentLogEntry(
  executionLog: any[],
  stepId: string,
  cutoff: string = LEGACY_LOG_CUTOFF,
): any | undefined {
  return executionLog.find(
    (entry: any) =>
      entry.step_id === stepId &&
      (entry.action === 'sent' ||
        (entry.action === 'completed' &&
          String(entry.executed_at || entry.ts || '') < cutoff)),
  );
}
