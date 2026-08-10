// Unsubscribe footer for campaign emails.
//
// The token is per (campaign, recipient) and is minted on first send by
// get_or_create_optout_token, so a follow-up carries the same link as the first
// email. Redeeming it writes a GLOBAL exclusion rule for the client, which the
// existing BEFORE INSERT trigger on unipile_sequence_executions already
// enforces — that is why nothing else in this worker needs to know about
// opt-outs.

import { supabase } from '../supabase';
import { config } from '../config';

export interface OptOutContext {
  enabled: boolean;
  /** Client-authored sentence placed before the link. */
  text?: string;
  sequenceId: string;
  clientId: string;
  contactId?: string | null;
  leadId?: string | null;
}

export const DEFAULT_OPTOUT_TEXT = "Not interested? You can";

/** Lets a client drop the link mid-sentence instead of taking the footer. */
const PLACEHOLDER = /\{\{\s*unsubscribe_link\s*\}\}/gi;

function escapeText(text: string): string {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

/** Never throws: a footer problem must not stop the email going out. */
export async function resolveOptOutToken(
  ctx: OptOutContext,
  email: string,
): Promise<string | null> {
  try {
    const { data, error } = await supabase.rpc('get_or_create_optout_token', {
      p_sequence_id: ctx.sequenceId,
      p_client_id: ctx.clientId,
      p_contact_id: ctx.contactId ?? null,
      p_lead_id: ctx.leadId ?? null,
      p_email: email,
    });
    if (error) {
      console.error('[optout] could not mint token:', error.message);
      return null;
    }
    return (data as string | null) || null;
  } catch (err: any) {
    console.error('[optout] token lookup threw:', err?.message || err);
    return null;
  }
}

export function optOutUrl(token: string): string {
  return `${config.appUrl.replace(/\/+$/, '')}/opt-out?token=${encodeURIComponent(token)}`;
}

/**
 * Append (or inline) the unsubscribe link.
 *
 * Runs on the HTML body AFTER plain-text escaping — escaping it would turn the
 * anchor into visible markup in the recipient's inbox.
 */
export function applyOptOut(htmlBody: string, url: string, text?: string): string {
  const anchor = `<a href="${url}" style="color:inherit;text-decoration:underline;">unsubscribe</a>`;

  // Inline placeholder wins: the client chose where it goes.
  const inlined = htmlBody.replace(PLACEHOLDER, anchor);
  if (inlined !== htmlBody) return inlined;

  const sentence = escapeText((text || '').trim() || DEFAULT_OPTOUT_TEXT);
  return (
    `${htmlBody}<br><br>` +
    `<div style="color:#8a8a8a;font-size:12px;line-height:1.5;">${sentence} ${anchor}.</div>`
  );
}
