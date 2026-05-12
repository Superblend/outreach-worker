/**
 * Phantom-message guard tests
 *
 * Covers:
 *  1. sendLinkedInMessage: allowInviteFallback=false skips the invite endpoint.
 *  2. isLinkedInMessageFollowUp: correctly identifies follow-ups via executionLog.
 *  3. isPhantomMessageResult: detects invitation_id-only results.
 *  4. Integration — exact original failure shape:
 *       executionLog has a completed linkedin_invitation (not message)
 *       + current step is linkedin_message
 *       + execution.chat_id is missing
 *     With the fix:  allowInviteFallback=false → invite endpoint never called →
 *                    result carries message_id (real send, not phantom).
 *     Safety net:    if somehow only invitation_id is returned,
 *                    isPhantomMessageResult flags it immediately.
 *  5. linkedin_invitation flows are unchanged (allowInviteFallback=true default).
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

// Pure helpers — no mocking required
import {
  isPhantomMessageResult,
  isLinkedInMessageFollowUp,
} from '../lib/linkedin-helpers';

// ---------------------------------------------------------------------------
// Module mocks for sendLinkedInMessage tests
// ---------------------------------------------------------------------------

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
    supabase: { url: 'https://test.supabase.co', anonKey: 'test-anon' },
  },
}));

vi.mock('../lib/unipile-fetch', () => ({
  unipileFetch: (...args: any[]) => mockUnipileFetch(...args),
}));

vi.mock('../lib/variable-replace', () => ({
  normalizeAndReplace: (msg: string) => msg,
}));

import { sendLinkedInMessage } from '../lib/unipile-send-linkedin-message';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
    neq: () => chain,
    in: () => chain,
    not: () => chain,
    is: () => chain,
    gt: () => chain,
    order: () => chain,
    limit: () => chain,
    single: () => Promise.resolve(result),
    maybeSingle: () => Promise.resolve(result),
    insert: () => Promise.resolve({ data: null, error: null }),
    update: () => chain,
    upsert: () => Promise.resolve({ data: null, error: null }),
  };
  return chain;
}

function activeAccount() {
  return makeSupabaseChain({
    data: { account_id: 'unipile-acct-1', status: 'active' },
    error: null,
  });
}

// ---------------------------------------------------------------------------
// 1. isPhantomMessageResult (uses real production code)
// ---------------------------------------------------------------------------

describe('isPhantomMessageResult', () => {
  it('flags invitation_id-only result as phantom', () => {
    expect(isPhantomMessageResult({ success: true, invitation_id: 'inv-001' })).toBe(true);
  });

  it('does not flag result that has message_id', () => {
    expect(isPhantomMessageResult({ success: true, message_id: 'msg-1', chat_id: 'c-1' })).toBe(false);
  });

  it('does not flag result that has chat_id but no message_id', () => {
    expect(isPhantomMessageResult({ success: true, chat_id: 'c-1' })).toBe(false);
  });

  it('does not flag failed result', () => {
    expect(isPhantomMessageResult({ success: false, invitation_id: 'inv-001' })).toBe(false);
  });

  it('does not flag null or undefined', () => {
    expect(isPhantomMessageResult(null)).toBe(false);
    expect(isPhantomMessageResult(undefined)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 2. isLinkedInMessageFollowUp (uses real production code)
// ---------------------------------------------------------------------------

describe('isLinkedInMessageFollowUp', () => {
  it('returns true when prior linkedin_invitation step completed', () => {
    const steps = [
      { id: 'step-inv', step_type: 'linkedin_invitation' },
      { id: 'step-msg', step_type: 'linkedin_message' },
    ];
    const log = [{ step_id: 'step-inv', action: 'completed' }];
    expect(isLinkedInMessageFollowUp(steps, log, 'step-msg')).toBe(true);
  });

  it('returns true when prior linkedin_message step completed', () => {
    const steps = [
      { id: 'msg-1', step_type: 'linkedin_message' },
      { id: 'msg-2', step_type: 'linkedin_message' },
    ];
    const log = [{ step_id: 'msg-1', action: 'completed' }];
    expect(isLinkedInMessageFollowUp(steps, log, 'msg-2')).toBe(true);
  });

  it('returns false when no prior linkedin step has completed', () => {
    const steps = [
      { id: 'step-inv', step_type: 'linkedin_invitation' },
      { id: 'step-msg', step_type: 'linkedin_message' },
    ];
    expect(isLinkedInMessageFollowUp(steps, [], 'step-msg')).toBe(false);
  });

  it('returns false for a lone first-touch linkedin_message', () => {
    const steps = [{ id: 'step-msg', step_type: 'linkedin_message' }];
    expect(isLinkedInMessageFollowUp(steps, [], 'step-msg')).toBe(false);
  });

  it('does not count the current step itself as a prior step', () => {
    const steps = [{ id: 'step-msg', step_type: 'linkedin_message' }];
    const log = [{ step_id: 'step-msg', action: 'completed' }];
    expect(isLinkedInMessageFollowUp(steps, log, 'step-msg')).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 3. sendLinkedInMessage — allowInviteFallback flag
// ---------------------------------------------------------------------------

describe('sendLinkedInMessage — allowInviteFallback', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSupabaseFrom.mockReturnValue(activeAccount());
  });

  it('calls invite endpoint when allowInviteFallback=true (default)', async () => {
    mockUnipileFetch
      .mockResolvedValueOnce(makeJsonResponse({ provider_id: 'prov-123' }))
      .mockResolvedValueOnce(makeJsonResponse({ id: 'inv-001' }));

    const result = await sendLinkedInMessage({
      account_id: 'acct-1',
      lead: { linkedin: 'https://linkedin.com/in/johndoe' },
      message: 'Hi',
    });

    const urls = mockUnipileFetch.mock.calls.map((c: any[]) => c[0] as string);
    expect(urls.some((u) => u.includes('/users/invite'))).toBe(true);
    expect(result.success).toBe(true);
    expect(result.invitation_id).toBe('inv-001');
  });

  it('never calls invite endpoint when allowInviteFallback=false', async () => {
    mockUnipileFetch
      .mockResolvedValueOnce(makeJsonResponse({ provider_id: 'prov-123' }))
      .mockResolvedValueOnce(
        makeJsonResponse({ items: [{ id: 'chat-99', attendee_provider_id: 'prov-123' }] }),
      )
      .mockResolvedValueOnce(makeJsonResponse({ id: 'msg-555' }));

    const result = await sendLinkedInMessage({
      account_id: 'acct-1',
      lead: { linkedin: 'https://linkedin.com/in/johndoe' },
      message: 'Follow-up',
      allowInviteFallback: false,
    });

    const urls = mockUnipileFetch.mock.calls.map((c: any[]) => c[0] as string);
    expect(urls.some((u) => u.includes('/users/invite'))).toBe(false);
    expect(result.success).toBe(true);
    expect(result.message_id).toBe('msg-555');
    expect(result.chat_id).toBe('chat-99');
  });

  it('skips to sendToChat immediately when chat_id is provided, regardless of allowInviteFallback', async () => {
    mockUnipileFetch.mockResolvedValueOnce(makeJsonResponse({ id: 'msg-direct' }));

    const result = await sendLinkedInMessage({
      account_id: 'acct-1',
      lead: { linkedin: 'https://linkedin.com/in/johndoe' },
      message: 'Hello',
      chat_id: 'existing-chat',
      allowInviteFallback: false,
    });

    const urls = mockUnipileFetch.mock.calls.map((c: any[]) => c[0] as string);
    expect(urls.some((u) => u.includes('/users/invite'))).toBe(false);
    expect(result.success).toBe(true);
    expect(result.message_id).toBe('msg-direct');
    expect(result.chat_id).toBe('existing-chat');
  });
});

// ---------------------------------------------------------------------------
// 4. Integration — exact original failure shape
//
//    Sequence: linkedin_invitation step → linkedin_message step
//    State:    invitation completed, execution.chat_id missing (webhook not received)
//    Old code: sendLinkedInMessage fell back to invite → returned invitation_id (phantom)
//    New code: isLinkedInMessageFollowUp=true → allowInviteFallback=false →
//              invite endpoint blocked → chat lookup / create → real message_id returned
// ---------------------------------------------------------------------------

describe('integration: prior linkedin_invitation → linkedin_message (original failure shape)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSupabaseFrom.mockReturnValue(activeAccount());
  });

  it('detects follow-up correctly when executionLog has a completed linkedin_invitation (not a message)', () => {
    const steps = [
      { id: 'step-inv', step_type: 'linkedin_invitation' },
      { id: 'step-msg', step_type: 'linkedin_message' },
    ];
    const executionLog = [
      {
        step_id: 'step-inv',
        step_type: 'linkedin_invitation',
        action: 'completed',
        executed_at: new Date().toISOString(),
      },
    ];

    expect(isLinkedInMessageFollowUp(steps, executionLog, 'step-msg')).toBe(true);
  });

  it('with allowInviteFallback=false (follow-up): invite endpoint is blocked, existing chat is found and used — result is a real message not a phantom', async () => {
    // execution.chat_id is missing — simulates the original bug precondition
    mockUnipileFetch
      .mockResolvedValueOnce(makeJsonResponse({ provider_id: 'prov-abc' }))             // profile lookup
      .mockResolvedValueOnce(
        makeJsonResponse({ items: [{ id: 'chat-abc', attendee_provider_id: 'prov-abc' }] }), // chat found
      )
      .mockResolvedValueOnce(makeJsonResponse({ id: 'msg-real-001' }));                  // send to chat

    const result = await sendLinkedInMessage({
      account_id: 'acct-linkedin',
      lead: { linkedin: 'https://linkedin.com/in/target' },
      message: 'Follow-up message',
      // chat_id intentionally absent — this is the missing-webhook scenario
      allowInviteFallback: false, // set by worker because isLinkedInMessageFollowUp=true
    });

    const urls = mockUnipileFetch.mock.calls.map((c: any[]) => c[0] as string);

    // Invite endpoint must never be reached
    expect(urls.some((u) => u.includes('/users/invite'))).toBe(false);

    // Result must be a real message send
    expect(result.success).toBe(true);
    expect(result.message_id).toBe('msg-real-001');
    expect(result.chat_id).toBe('chat-abc');

    // Confirm the phantom guard would NOT fire for this result
    expect(isPhantomMessageResult(result)).toBe(false);
  });

  it('with allowInviteFallback=false (follow-up): no existing chat → new chat created — still a real message not a phantom', async () => {
    mockUnipileFetch
      .mockResolvedValueOnce(makeJsonResponse({ provider_id: 'prov-abc' }))   // profile
      .mockResolvedValueOnce(makeJsonResponse({ items: [] }))                  // chat lookup empty
      .mockResolvedValueOnce(
        makeJsonResponse({ id: 'new-msg-id', chat_id: 'new-chat-id' }),        // create chat
      );

    const result = await sendLinkedInMessage({
      account_id: 'acct-linkedin',
      lead: { linkedin: 'https://linkedin.com/in/target' },
      message: 'Follow-up message',
      allowInviteFallback: false,
    });

    const urls = mockUnipileFetch.mock.calls.map((c: any[]) => c[0] as string);
    expect(urls.some((u) => u.includes('/users/invite'))).toBe(false);
    expect(result.success).toBe(true);
    expect(result.invitation_id).toBeUndefined();
    expect(isPhantomMessageResult(result)).toBe(false);
  });

  it('safety-net: if sendLinkedInMessage somehow returns invitation_id only, isPhantomMessageResult flags it regardless of follow-up status', () => {
    // This is the exact pre-fix shape — invitation_id with no message_id / chat_id
    const preFix = { success: true, invitation_id: 'phantom-inv-999' };
    // Post-fix shape produced by the chat send path
    const postFix = { success: true, message_id: 'msg-real', chat_id: 'chat-abc' };

    expect(isPhantomMessageResult(preFix)).toBe(true);   // this was the bug
    expect(isPhantomMessageResult(postFix)).toBe(false);  // this is the fix
  });
});

// ---------------------------------------------------------------------------
// 5. linkedin_invitation step flows are unchanged
//    (sendLinkedInMessage with allowInviteFallback=true default still calls invite)
// ---------------------------------------------------------------------------

describe('linkedin_invitation step — invite endpoint unchanged', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSupabaseFrom.mockReturnValue(activeAccount());
  });

  it('first-touch linkedin_message (no prior steps) still calls invite endpoint', async () => {
    mockUnipileFetch
      .mockResolvedValueOnce(makeJsonResponse({ provider_id: 'prov-789' }))
      .mockResolvedValueOnce(makeJsonResponse({ id: 'inv-xyz' }));

    const result = await sendLinkedInMessage({
      account_id: 'acct-1',
      lead: { linkedin: 'https://linkedin.com/in/newlead' },
      message: 'Connection request',
      // allowInviteFallback defaults to true
    });

    const urls = mockUnipileFetch.mock.calls.map((c: any[]) => c[0] as string);
    expect(urls.some((u) => u.includes('/users/invite'))).toBe(true);
    expect(result.invitation_id).toBe('inv-xyz');
  });
});
