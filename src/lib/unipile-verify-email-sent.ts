import { supabase } from '../supabase';
import { config } from '../config';
import { unipileFetch } from './unipile-fetch';

/**
 * Reconciliation lookup for the "indeterminate email send" case.
 *
 * When a send returns 502/503/504/timeout/network-error, we do NOT know whether
 * Unipile actually delivered the message (a gateway timeout means "no response",
 * not "not sent"). Before we ever re-send, we ask Unipile directly: is there a
 * message in this account's Sent folder, to this recipient, at/after the moment
 * we started the attempt? If yes, the email WAS delivered — we must not re-send.
 *
 * This is read-only (GET), so it runs even under DRY_RUN, and it only fires on
 * the rare unconfirmed path — never on the normal first-send hot path.
 */

export interface VerifyEmailSentParams {
  account_id: string;        // internal unipile_accounts.id
  recipientEmail: string;
  subject: string;           // subject we attempted (pre-"Re:"); matched loosely
  sentAfterISO: string;      // only consider messages sent at/after this instant
}

export interface VerifyEmailSentResult {
  delivered: boolean;
  provider_id?: string | null;
  matchedId?: string | null;
  reason: string;            // for logging: how we decided
}

// Sent-folder name/role heuristics across providers + common locales (hostnet.nl = NL).
const SENT_FOLDER_PATTERN = /sent|verzonden|gesend|envoy|enviad|inviat|gesendet/i;

// Allow a small negative tolerance so tiny clock skew between us and the mail
// server can't hide a genuinely-delivered message that is timestamped a hair
// before our recorded attempt time.
const SENT_AFTER_TOLERANCE_MS = 90_000;

function normalizeSubject(s: string | undefined | null): string {
  return (s || '')
    .replace(/^\s*(re|fw|fwd|aw|wg)\s*:\s*/gi, '') // strip reply/forward prefixes (multi-locale)
    .trim()
    .toLowerCase();
}

/** Defensive recipient extraction — Unipile shapes vary across versions/providers. */
function recipientsOf(email: any): string[] {
  const out: string[] = [];
  const push = (v: any) => {
    if (!v) return;
    if (typeof v === 'string') out.push(v);
    else if (v.identifier) out.push(v.identifier);
    else if (v.email) out.push(v.email);
  };
  const arrays = [email?.to_attendees, email?.to, email?.recipients, email?.to_recipients];
  for (const arr of arrays) {
    if (Array.isArray(arr)) arr.forEach(push);
    else if (arr) push(arr);
  }
  return out.map((e) => e.toLowerCase());
}

function dateOf(email: any): number | null {
  const raw = email?.date || email?.sent_at || email?.received_at || email?.timestamp;
  if (!raw) return null;
  const t = new Date(raw).getTime();
  return Number.isNaN(t) ? null : t;
}

function providerIdOf(email: any): string | null {
  return email?.provider_id || email?.message_id || email?.id || email?.object?.id || null;
}

async function unipileGetJson(url: string): Promise<any | null> {
  try {
    const res = await unipileFetch(url, {
      method: 'GET',
      headers: { 'X-API-KEY': config.unipile.apiKey },
    });
    if (!res.ok) {
      console.warn(`⚠️ [verify-email-sent] GET ${url} → HTTP ${res.status}`);
      await res.text().catch(() => undefined);
      return null;
    }
    return await res.json().catch(() => null);
  } catch (err: any) {
    console.warn(`⚠️ [verify-email-sent] GET ${url} threw: ${err?.message || err}`);
    return null;
  }
}

export async function verifyEmailSent(params: VerifyEmailSentParams): Promise<VerifyEmailSentResult> {
  const { account_id, recipientEmail, subject, sentAfterISO } = params;
  const apiUrl = config.unipile.apiUrl;
  const recipient = (recipientEmail || '').toLowerCase();
  const sentAfter = new Date(sentAfterISO).getTime() - SENT_AFTER_TOLERANCE_MS;

  if (!recipient) return { delivered: false, reason: 'no_recipient' };

  const { data: accountRow, error: accountError } = await supabase
    .from('unipile_accounts')
    .select('account_id')
    .eq('id', account_id)
    .single();

  if (accountError || !accountRow?.account_id) {
    return { delivered: false, reason: `account_lookup_failed:${accountError?.message || 'not_found'}` };
  }
  const uid = accountRow.account_id;

  // 1. Locate the Sent folder (best-effort; we fall back to an unscoped listing).
  let sentFolderId: string | null = null;
  const foldersData = await unipileGetJson(`${apiUrl}/api/v1/folders?account_id=${uid}`);
  const folders = foldersData?.items || foldersData || [];
  if (Array.isArray(folders)) {
    const sent = folders.find((f: any) =>
      String(f?.role || '').toLowerCase() === 'sent' ||
      SENT_FOLDER_PATTERN.test(String(f?.name || f?.display_name || '')));
    sentFolderId = sent?.id || sent?.folder_id || null;
  }

  // 2. List recent messages (prefer the Sent folder; otherwise match by sender below).
  const listUrl = sentFolderId
    ? `${apiUrl}/api/v1/emails?account_id=${uid}&folder=${sentFolderId}&limit=50`
    : `${apiUrl}/api/v1/emails?account_id=${uid}&limit=50`;
  const emailsData = await unipileGetJson(listUrl);
  const emails = emailsData?.items || emailsData || [];
  if (!Array.isArray(emails) || emails.length === 0) {
    return { delivered: false, reason: sentFolderId ? 'sent_folder_empty' : 'no_emails_listed' };
  }

  // One-time structural log so we can confirm field names against a live account
  // during testing without dumping any credentials.
  console.log(`🔎 [verify-email-sent] listed ${emails.length} msgs (folder=${sentFolderId || 'none'}); sample keys=${JSON.stringify(Object.keys(emails[0] || {}))}`);

  const wantSubject = normalizeSubject(subject);
  let subjectMismatchFallback: any = null;

  for (const email of emails) {
    const when = dateOf(email);
    if (when !== null && when < sentAfter) continue;             // too old to be our attempt
    if (!recipientsOf(email).includes(recipient)) continue;       // not to our lead

    // Recipient + time window is already a strong signal. Use subject to disambiguate
    // when multiple messages went to the same recipient in the window.
    if (!wantSubject || normalizeSubject(email?.subject) === wantSubject) {
      return {
        delivered: true,
        provider_id: providerIdOf(email),
        matchedId: email?.id || null,
        reason: 'matched_recipient_time_subject',
      };
    }
    subjectMismatchFallback = subjectMismatchFallback || email;
  }

  // Recipient + window matched but subject differed. Treat as delivered anyway —
  // erring toward "do not re-send" is the safe direction for avoiding duplicates,
  // and a message to this exact recipient from this account inside the window is
  // almost certainly ours (subject vars/threading can shift the visible subject).
  if (subjectMismatchFallback) {
    return {
      delivered: true,
      provider_id: providerIdOf(subjectMismatchFallback),
      matchedId: subjectMismatchFallback?.id || null,
      reason: 'matched_recipient_time_subject_differs',
    };
  }

  return { delivered: false, reason: 'no_match' };
}
