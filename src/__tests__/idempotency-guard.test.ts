/**
 * Idempotency guard — dual-read predicate tests
 *
 * Covers the four cases that must hold before deploying the A4 idempotency fix:
 *  (a) legacy action:'completed' with ts BEFORE cutoff  → skip (no re-execute)
 *  (b) legacy action:'completed' with ts AFTER cutoff   → re-execute (post-deploy completed ≠ sent)
 *  (c) action:'sent', any ts                            → skip (new anchor, no ts gate)
 *  (d) missing / null / epoch-ms ts on completed entry  → skip (safe default, no dup send)
 */

import { describe, it, expect } from 'vitest';
import { findSentLogEntry, LEGACY_LOG_CUTOFF } from '../lib/idempotency-helpers';

const STEP_ID = 'step-abc';
const PRE_CUTOFF_TS  = '2026-05-12T14:59:59.999Z'; // 1 ms before cutoff
const POST_CUTOFF_TS = '2026-05-12T15:00:00.001Z'; // 1 ms after cutoff

// ── (a) Legacy completed BEFORE cutoff → treated as already-sent ────────────

describe('findSentLogEntry — legacy completed pre-cutoff', () => {
  it('returns the entry when action=completed and executed_at is before cutoff', () => {
    const log = [{ step_id: STEP_ID, action: 'completed', executed_at: PRE_CUTOFF_TS }];
    expect(findSentLogEntry(log, STEP_ID)).toBeDefined();
  });

  it('returns the entry when action=completed and ts (not executed_at) is before cutoff', () => {
    const log = [{ step_id: STEP_ID, action: 'completed', ts: PRE_CUTOFF_TS }];
    expect(findSentLogEntry(log, STEP_ID)).toBeDefined();
  });

  it('prefers executed_at over ts when both are present and executed_at is pre-cutoff', () => {
    // executed_at pre-cutoff wins even when ts is post-cutoff
    const log = [{ step_id: STEP_ID, action: 'completed', executed_at: PRE_CUTOFF_TS, ts: POST_CUTOFF_TS }];
    expect(findSentLogEntry(log, STEP_ID)).toBeDefined();
  });
});

// ── (b) Legacy completed AFTER cutoff → allows re-execution ─────────────────

describe('findSentLogEntry — legacy completed post-cutoff', () => {
  it('returns undefined when action=completed and executed_at is after cutoff', () => {
    const log = [{ step_id: STEP_ID, action: 'completed', executed_at: POST_CUTOFF_TS }];
    expect(findSentLogEntry(log, STEP_ID)).toBeUndefined();
  });

  it('returns undefined when action=completed and ts is after cutoff and executed_at absent', () => {
    const log = [{ step_id: STEP_ID, action: 'completed', ts: POST_CUTOFF_TS }];
    expect(findSentLogEntry(log, STEP_ID)).toBeUndefined();
  });

  it('returns undefined when no matching entry exists at all', () => {
    expect(findSentLogEntry([], STEP_ID)).toBeUndefined();
  });

  it('does not match on a different step_id', () => {
    const log = [{ step_id: 'other-step', action: 'completed', executed_at: PRE_CUTOFF_TS }];
    expect(findSentLogEntry(log, STEP_ID)).toBeUndefined();
  });
});

// ── (c) action:'sent' always skips — no timestamp gate ───────────────────────

describe('findSentLogEntry — action:sent (new anchor)', () => {
  it('returns the entry for action=sent with a pre-cutoff ts', () => {
    const log = [{ step_id: STEP_ID, action: 'sent', ts: PRE_CUTOFF_TS }];
    expect(findSentLogEntry(log, STEP_ID)).toBeDefined();
  });

  it('returns the entry for action=sent with a post-cutoff ts', () => {
    const log = [{ step_id: STEP_ID, action: 'sent', ts: POST_CUTOFF_TS }];
    expect(findSentLogEntry(log, STEP_ID)).toBeDefined();
  });

  it('returns the entry for action=sent with no ts at all', () => {
    const log = [{ step_id: STEP_ID, action: 'sent' }];
    expect(findSentLogEntry(log, STEP_ID)).toBeDefined();
  });

  it('does not skip for action=started', () => {
    const log = [{ step_id: STEP_ID, action: 'started', ts: PRE_CUTOFF_TS }];
    expect(findSentLogEntry(log, STEP_ID)).toBeUndefined();
  });

  it('does not skip for action=error', () => {
    const log = [{ step_id: STEP_ID, action: 'error', ts: PRE_CUTOFF_TS }];
    expect(findSentLogEntry(log, STEP_ID)).toBeUndefined();
  });
});

// ── (d) Missing / null / epoch-ms ts on completed → safe default (skip) ──────

describe('findSentLogEntry — malformed or missing ts on completed entries', () => {
  it('treats missing ts as pre-cutoff → skip (safe: no dup send)', () => {
    const log = [{ step_id: STEP_ID, action: 'completed' }];
    // '' < LEGACY_LOG_CUTOFF is true → treated as pre-cutoff
    expect(findSentLogEntry(log, STEP_ID)).toBeDefined();
  });

  it('treats null ts as pre-cutoff → skip', () => {
    const log = [{ step_id: STEP_ID, action: 'completed', ts: null }];
    expect(findSentLogEntry(log, STEP_ID)).toBeDefined();
  });

  it('treats epoch-ms number ts as pre-cutoff → skip (String() prevents NaN coercion)', () => {
    // Without String(): number < string → JS coerces string to number → NaN → always false → re-execute (bug).
    // With String(): String(1747065600000) = '1747065600000' → '1' < '2' → true → skip (safe).
    const log = [{ step_id: STEP_ID, action: 'completed', ts: 1_747_065_600_000 }];
    expect(findSentLogEntry(log, STEP_ID)).toBeDefined();
  });

  it('treats epoch-ms string ts as pre-cutoff → skip', () => {
    const log = [{ step_id: STEP_ID, action: 'completed', ts: '1747065600000' }];
    expect(findSentLogEntry(log, STEP_ID)).toBeDefined();
  });
});

// ── Cutoff constant sanity ────────────────────────────────────────────────────

describe('LEGACY_LOG_CUTOFF constant', () => {
  it('is a valid ISO 8601 date string', () => {
    expect(new Date(LEGACY_LOG_CUTOFF).toISOString()).toBe(LEGACY_LOG_CUTOFF);
  });

  it('is in 2026 (confirms it has not already been removed)', () => {
    expect(LEGACY_LOG_CUTOFF.startsWith('2026-')).toBe(true);
  });
});
