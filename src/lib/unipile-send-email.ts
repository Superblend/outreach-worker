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
    tracking: { opens: true, links: true },
  };

  if (isReply && in_reply_to_message_id) {
    payload.reply_to = in_reply_to_message_id;
  }

  const res = await unipileFetch(`${apiUrl}/api/v1/emails`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-KEY': config.unipile.apiKey,
    },
    body: JSON.stringify(payload),
  });

  const data = await res.json().catch(() => null);

  if (!res.ok) {
    return { success: false, error: data?.error || data?.message || res.statusText };
  }

  return {
    success: true,
    provider_id: data?.provider_id || data?.object?.provider_id || data?.id,
    tracking_id: data?.tracking_id || data?.object?.tracking_id,
    subject,
    body: htmlBody,
    was_reply: isReply,
  };
}
