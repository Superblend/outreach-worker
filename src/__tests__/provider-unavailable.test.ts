/**
 * Provider-outage classification.
 *
 * Regression: Unipile answers `422 errors/provider_unreachable` — "Provider
 * unreachable, try again later." — when it cannot reach the upstream mailbox
 * provider (a stale Google OAuth grant, in the case that surfaced this). The
 * worker classified that as an ordinary non-retryable failure and completed the
 * execution as `email_failed` on the FIRST attempt, so one client lost 100+
 * leads across an 11-day outage without a single email being sent.
 *
 * These tests lock in that the message Unipile actually sends is recognised as
 * a provider outage, in both the raw and the wrapped-by-sendEmail form, and
 * that genuine hard rejections are NOT swept up with it.
 */

import { describe, it, expect } from 'vitest';
import { isProviderUnavailableError } from '../lib/error-classification';

describe('isProviderUnavailableError', () => {
  it('matches the exact string sendEmail builds from a Unipile 422', () => {
    // Verbatim from prod: unipile_step_results.error_message
    expect(
      isProviderUnavailableError('HTTP 422: Provider unreachable, try again later.'),
    ).toBe(true);
  });

  it('matches the raw provider body type slug', () => {
    expect(
      isProviderUnavailableError(
        '{"status":422,"type":"errors/provider_unreachable","title":"Provider unreachable"}',
      ),
    ).toBe(true);
  });

  it('is case-insensitive', () => {
    expect(isProviderUnavailableError('PROVIDER UNREACHABLE')).toBe(true);
    expect(isProviderUnavailableError('Try Again Later')).toBe(true);
  });

  it('does not match hard rejections that should stay terminal', () => {
    for (const msg of [
      'HTTP 422: Missing required subject for new email',
      'Account abc is not active (disconnected)',
      'follow_up_threading_rejected: HTTP 422',
      'Unauthorized',
      'profile not found',
      'HTTP 400: invalid recipient address',
    ]) {
      expect(isProviderUnavailableError(msg), msg).toBe(false);
    }
  });

  it('does not match an empty error', () => {
    expect(isProviderUnavailableError('')).toBe(false);
  });
});