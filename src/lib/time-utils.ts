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
