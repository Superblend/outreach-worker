/**
 * Overnight-window sending tests.
 *
 * Regression: a sequence configured with an overnight window (start > end,
 * e.g. 16:00 → 11:30 next morning) produced ZERO sends because every window
 * check used `cur >= start && cur <= end`, which is unsatisfiable when the
 * window wraps past midnight. The worker perpetually rescheduled each due
 * execution to "tomorrow's window open" and never dispatched.
 *
 * These tests lock in:
 *   - isInDailyWindow: membership for same-day AND overnight windows.
 *   - enforceDailyWindow: in-window passes through; out-of-window returns the
 *     correct next window-open instant (timezone-aware).
 *   - isWithinActiveWindow: overnight windows are gated (not skipped), same-day
 *     behaviour unchanged.
 *
 * All timezone cases use Europe/Paris in July (CEST = UTC+2), matching the
 * production sequence that surfaced the bug.
 */

import { describe, it, expect } from 'vitest';
import {
  isInDailyWindow,
  enforceDailyWindow,
  isWithinActiveWindow,
} from '../lib/time-utils';

const min = (h: number, m = 0) => h * 60 + m;

// Paris (CEST, UTC+2) wall-clock → the UTC instant to feed the helpers.
const parisJuly = (h: number, m = 0, day = 9) =>
  new Date(`2026-07-${String(day).padStart(2, '0')}T${String(h - 2).padStart(2, '0')}:${String(m).padStart(2, '0')}:00.000Z`);

describe('isInDailyWindow', () => {
  describe('same-day window 09:00–18:00', () => {
    const s = min(9), e = min(18);
    it('inside', () => expect(isInDailyWindow(min(12), s, e)).toBe(true));
    it('at start boundary', () => expect(isInDailyWindow(min(9), s, e)).toBe(true));
    it('at end boundary (inclusive)', () => expect(isInDailyWindow(min(18), s, e)).toBe(true));
    it('before start', () => expect(isInDailyWindow(min(8), s, e)).toBe(false));
    it('after end', () => expect(isInDailyWindow(min(19), s, e)).toBe(false));
  });

  describe('overnight window 16:00–11:30 (wraps midnight)', () => {
    const s = min(16), e = min(11, 30);
    it('evening (17:00) inside', () => expect(isInDailyWindow(min(17), s, e)).toBe(true));
    it('at start boundary (16:00)', () => expect(isInDailyWindow(min(16), s, e)).toBe(true));
    it('just before start (15:59) outside', () => expect(isInDailyWindow(min(15, 59), s, e)).toBe(false));
    it('midnight (00:00) inside', () => expect(isInDailyWindow(0, s, e)).toBe(true));
    it('early morning (02:00) inside', () => expect(isInDailyWindow(min(2), s, e)).toBe(true));
    it('morning (10:00) inside', () => expect(isInDailyWindow(min(10), s, e)).toBe(true));
    it('at end boundary (11:30) inside', () => expect(isInDailyWindow(min(11, 30), s, e)).toBe(true));
    it('just after end (11:31) outside', () => expect(isInDailyWindow(min(11, 31), s, e)).toBe(false));
    it('midday gap (13:00) outside', () => expect(isInDailyWindow(min(13), s, e)).toBe(false));
  });

  it('degenerate window (start === end) is always open', () => {
    expect(isInDailyWindow(min(3), min(9), min(9))).toBe(true);
  });
});

describe('enforceDailyWindow (Europe/Paris)', () => {
  describe('overnight 16:00–11:30 (Nue’s production window)', () => {
    it('evening 16:37 in-window → unchanged', () => {
      const t = parisJuly(16, 37);
      expect(enforceDailyWindow(t, '16:00', '11:30', 'Europe/Paris').getTime()).toBe(t.getTime());
    });
    it('early morning 02:00 in-window → unchanged', () => {
      const t = parisJuly(2);
      expect(enforceDailyWindow(t, '16:00', '11:30', 'Europe/Paris').getTime()).toBe(t.getTime());
    });
    it('morning 10:00 in-window → unchanged', () => {
      const t = parisJuly(10);
      expect(enforceDailyWindow(t, '16:00', '11:30', 'Europe/Paris').getTime()).toBe(t.getTime());
    });
    it('midday gap 13:00 → today 16:00 Paris (14:00 UTC), NOT tomorrow', () => {
      const t = parisJuly(13);
      expect(enforceDailyWindow(t, '16:00', '11:30', 'Europe/Paris').toISOString())
        .toBe('2026-07-09T14:00:00.000Z');
    });
  });

  describe('same-day 09:00–18:00 (regression guard — unchanged behaviour)', () => {
    it('midday 12:00 in-window → unchanged', () => {
      const t = parisJuly(12);
      expect(enforceDailyWindow(t, '09:00', '18:00', 'Europe/Paris').getTime()).toBe(t.getTime());
    });
    it('before start 08:00 → today 09:00 Paris (07:00 UTC)', () => {
      const t = parisJuly(8);
      expect(enforceDailyWindow(t, '09:00', '18:00', 'Europe/Paris').toISOString())
        .toBe('2026-07-09T07:00:00.000Z');
    });
    it('after end 19:00 → tomorrow 09:00 Paris (next-day 07:00 UTC)', () => {
      const t = parisJuly(19);
      expect(enforceDailyWindow(t, '09:00', '18:00', 'Europe/Paris').toISOString())
        .toBe('2026-07-10T07:00:00.000Z');
    });
    it('within 15-min past-end grace (18:10) → unchanged', () => {
      const t = parisJuly(18, 10);
      expect(enforceDailyWindow(t, '09:00', '18:00', 'Europe/Paris').getTime()).toBe(t.getTime());
    });
  });
});

describe('isWithinActiveWindow (overnight gating)', () => {
  const overnight = {
    timezone: 'Europe/Paris',
    active_days: null, // all days → isolate the time-window logic
    scheduled_start_time: '16:00',
    scheduled_end_time: '11:30',
  };

  it('evening 17:00 → ok', () => {
    expect(isWithinActiveWindow(overnight, parisJuly(17)).ok).toBe(true);
  });
  it('early morning 02:00 → ok', () => {
    expect(isWithinActiveWindow(overnight, parisJuly(2)).ok).toBe(true);
  });
  it('midday gap 13:00 → blocked (previously wrongly allowed)', () => {
    expect(isWithinActiveWindow(overnight, parisJuly(13)).ok).toBe(false);
  });

  it('same-day window still gates correctly', () => {
    const sameDay = { ...overnight, scheduled_start_time: '09:00', scheduled_end_time: '18:00' };
    expect(isWithinActiveWindow(sameDay, parisJuly(12)).ok).toBe(true);
    expect(isWithinActiveWindow(sameDay, parisJuly(8)).ok).toBe(false);
    expect(isWithinActiveWindow(sameDay, parisJuly(20)).ok).toBe(false);
  });
});
