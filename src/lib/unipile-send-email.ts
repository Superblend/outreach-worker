import { supabase } from '../supabase';
import { config } from '../config';
import { unipileFetch } from './unipile-fetch';
import { normalizeAndReplace } from './variable-replace';

interface SendEmailParams {
  account_id: string;
  lead: any;
  subject: string;
  body: string;
  use_html?: boolean;
  in_reply_to_message_id?: string;
  original_subject?: string;
}

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\n/g, '<br>');
}

export async function sendEmail(params: SendEmailParams): Promise<any> {
  const { account_id, lead, use_html, in_reply_to_message_id, original_subject } = params;
  const apiUrl = config.unipile.apiUrl;

  // Fetch real Unipile account_id and signature
  const { data: accountRow, error: accountError } = await supabase
    .from('unipile_accounts')
    .select('account_id, status, email_signature')
    .eq('id', account_id)
    .single();

  if (accountError || !accountRow) {
    return { success: false, error: `Account not found: ${account_id}` };
  }

  if (accountRow.status !== 'active') {
    return { success: false, error: `Account ${account_id} is not active (${accountRow.status})` };
  }

  const unipileAccountId = accountRow.account_id;
  const signature = accountRow.email_signature || '';

  // Replace template variables
  let subject = normalizeAndReplace(params.subject, lead);
  let body = normalizeAndReplace(params.body, lead, signature);

  // Determine if this is a reply
  const isReply = !!in_reply_to_message_id;
  let replySubject: string | undefined;

  if (isReply) {
    // Compute reply subject
    const baseSubject = original_subject || subject;
    replySubject = baseSubject.startsWith('Re:') ? baseSubject : `Re: ${baseSubject}`;
    subject = replySubject;
  } else {
    if (!subject.trim()) {
      return { success: false, error: 'Missing required subject for new email' };
    }
  }

  // Convert plain text to HTML if not already HTML
  let htmlBody = body;
  if (!use_html) {
    htmlBody = escapeHtml(body);
  }

  // Build email payload
  const payload: any = {
    account_id: unipileAccountId,
    subject,
    body: htmlBody,
    to: [{ display_name: `${lead.first_name || ''} ${lead.last_name || ''}`.trim(), identifier: lead.email }],
    body_type: 'HTML',
    tracking_options: { opens: true, links: true },
  };

  if (isReply && in_reply_to_message_id) {
    payload.reply_to = in_reply_to_message_id;
  }

  // ── TEST-ONLY simulation hook (inert unless env is set) ──────────────────────
  // Lets us drive the indeterminate/reconciliation path deterministically on
  // staging without a real Unipile outage. SIMULATE_EMAIL_INDETERMINATE_TO=<email>
  //   mode 'before_send' (default): return indeterminate WITHOUT sending  → tests re-send path
  //   mode 'after_send'           : really send, THEN return indeterminate → tests confirm-no-resend path
  const simTo = process.env.SIMULATE_EMAIL_INDETERMINATE_TO?.toLowerCase();
  const simMode = (process.env.SIMULATE_EMAIL_INDETERMINATE_MODE || 'before_send').toLowerCase();
  const isSim = !!simTo && (lead?.email || '').toLowerCase() === simTo;
  if (isSim && simMode === 'before_send') {
    console.warn(`[SIM] email to ${lead.email}: returning indeterminate WITHOUT sending`);
    return { success: false, indeterminate: true, error: 'simulated gateway time-out (before_send)' };
  }

  let res: Response;
  try {
    res = await unipileFetch(`${apiUrl}/api/v1/emails`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-KEY': config.unipile.apiKey,
      },
      body: JSON.stringify(payload),
    });
  } catch (err: any) {
    // Connection reset / abort / DNS — the request may or may not have been
    // delivered. Ambiguous → let the caller reconcile before re-sending.
    return { success: false, indeterminate: true, error: `network_error: ${err?.message || err}` };
  }

  const data = await res.json().catch(() => null);

  if (!res.ok) {
    const errMsg = data?.error || data?.message || res.statusText || `HTTP ${res.status}`;
    // Gateway/timeout classes mean "we got no clear answer" — the message may
    // have been delivered. Flag as indeterminate so the caller verifies against
    // the Sent folder before deciding to re-send (prevents duplicate deliveries).
    const indeterminate =
      [502, 503, 504, 408, 425, 522, 524].includes(res.status) ||
      /time-?out|gateway|temporarily unavailable/i.test(errMsg);
    return { success: false, indeterminate, httpStatus: res.status, error: errMsg };
  }

  if (isSim && simMode === 'after_send') {
    console.warn(`[SIM] email to ${lead.email}: really sent, but returning indeterminate (after_send)`);
    return { success: false, indeterminate: true, error: 'simulated gateway time-out (after_send)' };
  }

  return {
    success: true,
    provider_id: data?.provider_id || data?.object?.provider_id || data?.id,
    tracking_id: data?.tracking_id || data?.object?.id || data?.id,
    subject,
    body: htmlBody,
    was_reply: isReply,
  };
}
