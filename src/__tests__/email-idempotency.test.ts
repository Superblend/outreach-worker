/**
 * Email idempotency / reconciliation tests
 *
 * Covers the fix for the "504 duplicate send" bug (Rob/nlockd, 2026-06-29):
 *  1. sendEmail classifies outcomes: 200→success, 4xx→hard failure,
 *     504/503/408/network-throw→indeterminate (delivery unknown, DO NOT re-send blindly).
 *  2. sendEmail TEST simulation hook (env-gated) drives the indeterminate path.
 *  3. verifyEmailSent matches a delivered message by recipient + time-window (+ soft subject),
 *     and returns delivered=false when nothing matches — the signal used to decide resend.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

const mockUnipileFetch = vi.fn();
const mockSupabaseFrom = vi.fn();

vi.mock('../supabase', () => ({
  supabase: {
    from: (...args: any[]) => mockSupabaseFrom(...args),
    rpc: vi.fn().mockResolvedValue({ data: null, error: null }),
  },
  invokeEdgeFunction: vi.fn().mockResolvedValue({ data: null, error: null }),
}));

vi.mock('../config', () => ({
  config: {
    unipile: { apiUrl: 'https://api.unipile.test', apiKey: 'test-key' },
    dryRun: false,
  },
}));

vi.mock('../lib/unipile-fetch', () => ({
  unipileFetch: (...args: any[]) => mockUnipileFetch(...args),
}));

vi.mock('../lib/variable-replace', () => ({
  normalizeAndReplace: (msg: string) => msg,
}));

import { sendEmail } from '../lib/unipile-send-email';
import { verifyEmailSent } from '../lib/unipile-verify-email-sent';

function makeJsonResponse(body: unknown, ok = true, status = 200) {
  return {
    ok,
    status,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body)),
  };
}

function makeSupabaseChain(result = { data: null as any, error: null as any }) {
  const chain: any = {
    select: () => chain,
    eq: () => chain,
    not: () => chain,
    order: () => chain,
    limit: () => chain,
    single: () => Promise.resolve(result),
    maybeSingle: () => Promise.resolve(result),
  };
  return chain;
}

const activeAccount = () =>
  makeSupabaseChain({ data: { account_id: 'unipile-acct-1', status: 'active', email_signature: '' }, error: null });

const lead = { first_name: 'Seb', last_name: 'Kremer', email: 'sebastiaan@triplepro.nl' };

// ---------------------------------------------------------------------------
// 1. sendEmail outcome classification
// ---------------------------------------------------------------------------

describe('sendEmail — outcome classification', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSupabaseFrom.mockReturnValue(activeAccount());
    delete process.env.SIMULATE_EMAIL_INDETERMINATE_TO;
    delete process.env.SIMULATE_EMAIL_INDETERMINATE_MODE;
  });

  it('200 → success', async () => {
    mockUnipileFetch.mockResolvedValueOnce(makeJsonResponse({ provider_id: 'prov-1', tracking_id: 'trk-1' }));
    const r = await sendEmail({ account_id: 'a', lead, subject: 'Hi', body: 'x' });
    expect(r.success).toBe(true);
    expect(r.provider_id).toBe('prov-1');
    expect(r.indeterminate).toBeFalsy();
  });

  it('504 Gateway Time-out → indeterminate (not a hard failure)', async () => {
    mockUnipileFetch.mockResolvedValueOnce(makeJsonResponse({ message: 'Gateway Time-out' }, false, 504));
    const r = await sendEmail({ account_id: 'a', lead, subject: 'Hi', body: 'x' });
    expect(r.success).toBe(false);
    expect(r.indeterminate).toBe(true);
    expect(r.httpStatus).toBe(504);
  });

  it('503 Service Unavailable → indeterminate', async () => {
    mockUnipileFetch.mockResolvedValueOnce(makeJsonResponse({ message: 'Service Unavailable' }, false, 503));
    const r = await sendEmail({ account_id: 'a', lead, subject: 'Hi', body: 'x' });
    expect(r.indeterminate).toBe(true);
  });

  it('network throw (ECONNRESET) → indeterminate', async () => {
    mockUnipileFetch.mockRejectedValueOnce(new Error('ECONNRESET'));
    const r = await sendEmail({ account_id: 'a', lead, subject: 'Hi', body: 'x' });
    expect(r.success).toBe(false);
    expect(r.indeterminate).toBe(true);
  });

  it('400 Bad Request → hard failure (NOT indeterminate, so it will not be re-sent)', async () => {
    mockUnipileFetch.mockResolvedValueOnce(makeJsonResponse({ error: 'Bad Request' }, false, 400));
    const r = await sendEmail({ account_id: 'a', lead, subject: 'Hi', body: 'x' });
    expect(r.success).toBe(false);
    expect(r.indeterminate).toBe(false);
  });

  it('missing subject on a new email → hard failure, never hits the network', async () => {
    const r = await sendEmail({ account_id: 'a', lead, subject: '   ', body: 'x' });
    expect(r.success).toBe(false);
    expect(r.indeterminate).toBeFalsy();
    expect(mockUnipileFetch).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// 2. sendEmail TEST simulation hook (env-gated)
// ---------------------------------------------------------------------------

describe('sendEmail — simulation hook', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSupabaseFrom.mockReturnValue(activeAccount());
  });
  afterEach(() => {
    delete process.env.SIMULATE_EMAIL_INDETERMINATE_TO;
    delete process.env.SIMULATE_EMAIL_INDETERMINATE_MODE;
  });

  it('before_send: returns indeterminate WITHOUT sending', async () => {
    process.env.SIMULATE_EMAIL_INDETERMINATE_TO = 'sebastiaan@triplepro.nl';
    process.env.SIMULATE_EMAIL_INDETERMINATE_MODE = 'before_send';
    const r = await sendEmail({ account_id: 'a', lead, subject: 'Hi', body: 'x' });
    expect(r.indeterminate).toBe(true);
    expect(mockUnipileFetch).not.toHaveBeenCalled();
  });

  it('after_send: really sends, then returns indeterminate', async () => {
    process.env.SIMULATE_EMAIL_INDETERMINATE_TO = 'sebastiaan@triplepro.nl';
    process.env.SIMULATE_EMAIL_INDETERMINATE_MODE = 'after_send';
    mockUnipileFetch.mockResolvedValueOnce(makeJsonResponse({ provider_id: 'prov-real' }));
    const r = await sendEmail({ account_id: 'a', lead, subject: 'Hi', body: 'x' });
    expect(r.indeterminate).toBe(true);
    expect(mockUnipileFetch).toHaveBeenCalledTimes(1); // the real send happened
  });

  it('non-matching recipient is unaffected by the sim env', async () => {
    process.env.SIMULATE_EMAIL_INDETERMINATE_TO = 'someone-else@example.com';
    mockUnipileFetch.mockResolvedValueOnce(makeJsonResponse({ provider_id: 'prov-2' }));
    const r = await sendEmail({ account_id: 'a', lead, subject: 'Hi', body: 'x' });
    expect(r.success).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 3. verifyEmailSent — the "did it actually go out?" lookup
// ---------------------------------------------------------------------------

describe('verifyEmailSent', () => {
  const T0 = '2026-06-29T10:00:00.000Z';
  const foldersResp = makeJsonResponse({ items: [{ id: 'folder-sent', role: 'sent', name: 'Sent' }] });

  beforeEach(() => {
    vi.clearAllMocks();
    mockSupabaseFrom.mockReturnValue(makeSupabaseChain({ data: { account_id: 'unipile-acct-1' }, error: null }));
  });

  it('delivered: message to recipient sent after attempt time, subject matches', async () => {
    mockUnipileFetch
      .mockResolvedValueOnce(foldersResp)
      .mockResolvedValueOnce(makeJsonResponse({ items: [
        { id: 'm1', provider_id: 'prov-xyz', date: '2026-06-29T10:00:30.000Z',
          subject: 'Context bij LinkedIn connectie',
          to_attendees: [{ identifier: 'sebastiaan@triplepro.nl' }] },
      ] }));

    const r = await verifyEmailSent({
      account_id: 'a', recipientEmail: 'sebastiaan@triplepro.nl',
      subject: 'Context bij LinkedIn connectie', sentAfterISO: T0,
    });
    expect(r.delivered).toBe(true);
    expect(r.provider_id).toBe('prov-xyz');
  });

  it('delivered even if subject differs (recipient + window is decisive; err toward no-resend)', async () => {
    mockUnipileFetch
      .mockResolvedValueOnce(foldersResp)
      .mockResolvedValueOnce(makeJsonResponse({ items: [
        { id: 'm2', provider_id: 'prov-2', date: '2026-06-29T10:01:00.000Z',
          subject: 'Re: something threaded', to: [{ identifier: 'sebastiaan@triplepro.nl' }] },
      ] }));

    const r = await verifyEmailSent({
      account_id: 'a', recipientEmail: 'sebastiaan@triplepro.nl',
      subject: 'Context bij LinkedIn connectie', sentAfterISO: T0,
    });
    expect(r.delivered).toBe(true);
  });

  it('NOT delivered: only messages to OTHER recipients', async () => {
    mockUnipileFetch
      .mockResolvedValueOnce(foldersResp)
      .mockResolvedValueOnce(makeJsonResponse({ items: [
        { id: 'm3', date: '2026-06-29T10:02:00.000Z', subject: 'Hi', to_attendees: [{ identifier: 'other@x.com' }] },
      ] }));

    const r = await verifyEmailSent({
      account_id: 'a', recipientEmail: 'sebastiaan@triplepro.nl', subject: 'Hi', sentAfterISO: T0,
    });
    expect(r.delivered).toBe(false);
  });

  it('NOT delivered: message to recipient but sent BEFORE the attempt (an older email in the thread)', async () => {
    mockUnipileFetch
      .mockResolvedValueOnce(foldersResp)
      .mockResolvedValueOnce(makeJsonResponse({ items: [
        { id: 'm4', date: '2026-06-20T08:00:00.000Z', subject: 'Older', to_attendees: [{ identifier: 'sebastiaan@triplepro.nl' }] },
      ] }));

    const r = await verifyEmailSent({
      account_id: 'a', recipientEmail: 'sebastiaan@triplepro.nl', subject: 'Older', sentAfterISO: T0,
    });
    expect(r.delivered).toBe(false);
  });

  it('NOT delivered: empty Sent folder', async () => {
    mockUnipileFetch
      .mockResolvedValueOnce(foldersResp)
      .mockResolvedValueOnce(makeJsonResponse({ items: [] }));

    const r = await verifyEmailSent({
      account_id: 'a', recipientEmail: 'sebastiaan@triplepro.nl', subject: 'Hi', sentAfterISO: T0,
    });
    expect(r.delivered).toBe(false);
  });
});
