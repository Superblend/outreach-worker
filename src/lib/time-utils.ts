/**
 * Reliable timezone-aware time utilities using Intl.DateTimeFormat.formatToParts().
 *
 * WHY NOT toLocaleString():
 *   toLocaleString('en-US', { hour: 'numeric', minute: 'numeric', hour12: false })
 *   returns "2 AM" (no colon) on some Node.js Docker images when the bundled ICU
 *   doesn't honour hour12:false for en-US. split(':') then produces a single-element
 *   array, lm = parseInt(undefined) = NaN, and NaN < startMin || NaN > endMin is
 *   always false — the time-window filter silently lets everything through.
 *
 *   formatToParts() returns named { type, value } objects regardless of locale
 *   formatting, so there is no string-splitting or locale-dependent output to worry about.
 */

/** 0 (Sun) – 6 (Sat) indexed weekday abbreviations from en-US Intl. */
const DAY_ABBREVS: Record<string, number> = {
  Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6,
};

/**
 * Returns minutes since midnight (0–1439) in the given IANA timezone.
 * e.g. 09:30 local → 570
 */
export function localMinutesOfDay(date: Date, timezone: string): number {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone,
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(date);
  const h = parseInt(parts.find(p => p.type === 'hour')!.value, 10);
  const m = parseInt(parts.find(p => p.type === 'minute')!.value, 10);
  // A handful of ICU versions emit 24 for midnight instead of 00; normalise.
  return (h === 24 ? 0 : h) * 60 + m;
}

/**
 * Returns the local calendar date as YYYY-MM-DD in the given IANA timezone.
 * Uses en-CA locale which always produces ISO 8601 date format.
 */
export function localDateString(date: Date, timezone: string): string {
  return date.toLocaleDateString('en-CA', { timeZone: timezone });
}

/**
 * Returns 0 (Sun) – 6 (Sat) for the day of week in the given IANA timezone.
 */
export function localWeekday(date: Date, timezone: string): number {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone,
    weekday: 'short',
  }).formatToParts(date);
  const abbrev = (parts.find(p => p.type === 'weekday')?.value ?? '').substring(0, 3);
  return DAY_ABBREVS[abbrev] ?? date.getUTCDay();
}

const DAY_ABBREV_NAMES = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

/**
 * Membership test for a daily sending window, expressed in minutes-since-midnight.
 * Handles both same-day windows (start <= end, e.g. 09:00–18:00) and overnight
 * windows that wrap past midnight (start > end, e.g. 16:00–11:30 next morning).
 *
 * End is inclusive. A degenerate window where start === end is treated as
 * always-open (24h) to preserve the "no gating" behaviour of a zero-length span.
 */
export function isInDailyWindow(curMin: number, startMin: number, endMin: number): boolean {
  if (startMin === endMin) return true;              // degenerate → all-day
  return startMin < endMin
    ? curMin >= startMin && curMin <= endMin          // same-day
    : curMin >= startMin || curMin <= endMin;         // overnight (wraps midnight)
}

/**
 * Converts a local wall-clock time (YYYY-MM-DD + hour:minute in `timezone`) to the
 * corresponding UTC instant. Canonical version of the per-file convertToUTC copies.
 */
export function convertLocalToUtc(dateStr: string, hour: number, minute: number, timezone: string): Date {
  const timeStr = `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:00`;
  const date = new Date(`${dateStr}T${timeStr}.000Z`);
  const utcDate = new Date(date.toLocaleString('en-US', { timeZone: 'UTC' }));
  const tzDate = new Date(date.toLocaleString('en-US', { timeZone: timezone }));
  const offsetMs = tzDate.getTime() - utcDate.getTime();
  return new Date(date.getTime() - offsetMs);
}

/**
 * Given a proposed send time, returns the same time if it falls within the daily
 * window (with a 15-minute past-end grace for queue-backlog stragglers), otherwise
 * the next window-open instant. Overnight-aware via {@link isInDailyWindow}.
 *
 * The "next window-open" target is today when we're before the window start
 * (covers both a same-day morning and the midday gap of an overnight window) and
 * tomorrow when we're already past a same-day window's end.
 */
export function enforceDailyWindow(
  proposedTime: Date,
  startStr: string,
  endStr: string,
  timezone: string,
): Date {
  const [startH, startM] = startStr.split(':').map(Number);
  const [endH, endM] = endStr.split(':').map(Number);
  const startMin = startH * 60 + startM;
  const endMin = endH * 60 + endM;
  const curMin = localMinutesOfDay(proposedTime, timezone);

  if (isInDailyWindow(curMin, startMin, endMin)) return proposedTime;
  // 15-min grace just past window end (only reachable for same-day / overnight morning close).
  if (curMin > endMin && curMin <= endMin + 15) return proposedTime;

  // Before start (same-day morning, or overnight midday gap) → today; else past a
  // same-day end → tomorrow. Uses local date so a TZ offset can't shift the day.
  const targetDate = curMin < startMin
    ? proposedTime
    : new Date(proposedTime.getTime() + 86_400_000);
  return convertLocalToUtc(localDateString(targetDate, timezone), startH, startM, timezone);
}

/**
 * Returns { ok: true } if `now` falls within the sequence's active_days and sending window
 * (both evaluated in the sequence's timezone), or { ok: false, reason } otherwise.
 *
 * Edge cases:
 *   - active_days null/empty → treated as all days active
 *   - scheduled_start_time/end_time null → only day check applies
 *   - end_time < start_time (overnight) → window wraps past midnight, gated via isInDailyWindow
 *   - timezone null → 'UTC'
 */
export function isWithinActiveWindow(
  seq: {
    timezone: string | null;
    active_days: number[] | null;
    scheduled_start_time: string | null;
    scheduled_end_time: string | null;
  },
  now: Date = new Date(),
): { ok: true } | { ok: false; reason: string } {
  const tz = seq.timezone || 'UTC';
  const dow = localWeekday(now, tz);
  const activeDays = seq.active_days?.length ? seq.active_days : [0, 1, 2, 3, 4, 5, 6];

  if (!activeDays.includes(dow)) {
    return { ok: false, reason: `inactive_day:${DAY_ABBREV_NAMES[dow]}` };
  }

  if (seq.scheduled_start_time && seq.scheduled_end_time) {
    const nowMin = localMinutesOfDay(now, tz);
    const [sh, sm] = seq.scheduled_start_time.split(':').map(Number);
    const [eh, em] = seq.scheduled_end_time.split(':').map(Number);
    const startMin = sh * 60 + sm;
    const endMin = eh * 60 + em;
    // Overnight windows (start > end) wrap past midnight and are gated correctly
    // by isInDailyWindow — no longer skipped.
    if (!isInDailyWindow(nowMin, startMin, endMin)) {
      const hh = Math.floor(nowMin / 60);
      const mm = nowMin % 60;
      return { ok: false, reason: `outside_window:${hh}:${String(mm).padStart(2, '0')}` };
    }
  }

  return { ok: true };
}

/**
 * Returns a UTC ISO string for the next valid send time: the earliest future moment
 * that falls on an active day, at or after scheduled_start_time, in the sequence's timezone.
 * Walks forward up to 8 days; falls back to 24 h from now if no active day is found.
 */
export function nextValidSendUtc(
  seq: {
    timezone: string | null;
    active_days: number[] | null;
    scheduled_start_time: string | null;
  },
  now: Date = new Date(),
): string {
  const tz = seq.timezone || 'UTC';
  const activeDays = seq.active_days?.length ? seq.active_days : [0, 1, 2, 3, 4, 5, 6];
  const [sh, sm] = (seq.scheduled_start_time || '09:00:00').split(':').map(Number);

  for (let i = 0; i < 8; i++) {
    const probe = new Date(now.getTime() + i * 86_400_000);
    if (!activeDays.includes(localWeekday(probe, tz))) continue;
    const ymd = localDateString(probe, tz); // YYYY-MM-DD in tz
    // Convert local wall-clock time to UTC by computing the TZ offset at that instant.
    const localIso = `${ymd}T${String(sh).padStart(2, '0')}:${String(sm).padStart(2, '0')}:00`;
    const asUtc = new Date(localIso + 'Z');
    const tzWall = new Date(asUtc.toLocaleString('en-US', { timeZone: tz }));
    const utcWall = new Date(asUtc.toLocaleString('en-US', { timeZone: 'UTC' }));
    const offsetMs = tzWall.getTime() - utcWall.getTime();
    const utcInstant = new Date(asUtc.getTime() - offsetMs);
    if (utcInstant.getTime() > now.getTime()) return utcInstant.toISOString();
  }
  return new Date(now.getTime() + 86_400_000).toISOString();
}
