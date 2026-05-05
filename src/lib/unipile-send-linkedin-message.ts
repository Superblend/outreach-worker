import { supabase } from '../supabase';
import { config } from '../config';
import { unipileFetch } from './unipile-fetch';
import { normalizeAndReplace } from './variable-replace';

interface SendLinkedInMessageParams {
  account_id: string;
  lead: any;
  message: string;
  use_inmail?: boolean;
  chat_id?: string;
  /** When false, the LinkedIn invite endpoint is never called even if no chat_id is known.
   *  Use for follow-up linkedin_message steps to prevent accidental invitation sends. */
  allowInviteFallback?: boolean;
}

function extractLinkedInIdentifier(lead: any): string | null {
  const url = lead.linkedin || lead.linkedin_url || '';
  if (!url) return null;
  const match = url.match(/linkedin\.com\/in\/([^/?]+)/);
  if (match && match[1]) return match[1].replace(/\/$/, '');
  return null;
}

export async function sendLinkedInMessage(params: SendLinkedInMessageParams): Promise<any> {
  const { account_id, lead, use_inmail, allowInviteFallback = true } = params;
  const apiUrl = config.unipile.apiUrl;

  // Resolve the Unipile account
  const { data: accountRow, error: accountError } = await supabase
    .from('unipile_accounts')
    .select('account_id, status')
    .eq('id', account_id)
    .single();

  if (accountError || !accountRow) {
    return { success: false, error: `Account not found: ${account_id}` };
  }

  if (accountRow.status !== 'active') {
    return { success: false, error: `Account ${account_id} is not active (${accountRow.status})` };
  }

  const unipileAccountId = accountRow.account_id;
  const personalizedMessage = normalizeAndReplace(params.message || '', lead);

  // Step 3: If a chat_id is already known, skip straight to send
  const existingChatId = params.chat_id;
  if (existingChatId) {
    return sendToChat({
      apiUrl, unipileAccountId, chatId: existingChatId,
      personalizedMessage, use_inmail,
      providerId: null, publicIdentifier: null,
    });
  }

  // Step 1: Resolve provider_id from LinkedIn profile
  const publicIdentifier = extractLinkedInIdentifier(lead);
  if (!publicIdentifier) {
    return { success: false, error: 'Missing LinkedIn URL on lead' };
  }

  const profileRes = await unipileFetch(
    `${apiUrl}/api/v1/users/${publicIdentifier}?account_id=${unipileAccountId}`,
    { headers: { 'X-API-KEY': config.unipile.apiKey } }
  );

  if (!profileRes.ok) {
    const errText = await profileRes.text().catch(() => '');
    let errMsg = `Profile fetch failed (${profileRes.status})`;
    try { errMsg = JSON.parse(errText)?.message || JSON.parse(errText)?.error || errMsg; } catch {}
    return { success: false, error: errMsg };
  }

  const profileData = await profileRes.json().catch(() => null);
  const providerId: string | null = profileData?.provider_id || null;

  if (!providerId) {
    return { success: false, error: 'Could not get provider_id from LinkedIn profile' };
  }

  // Step 4: Try invite endpoint (skipped for follow-up steps where allowInviteFallback=false)
  if (allowInviteFallback) {
    const inviteRes = await unipileFetch(`${apiUrl}/api/v1/users/invite`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-KEY': config.unipile.apiKey },
      body: JSON.stringify({ account_id: unipileAccountId, provider_id: providerId, message: personalizedMessage }),
    });

    if (inviteRes.ok) {
      const inviteData = await inviteRes.json().catch(() => null);
      const inviteId = inviteData?.id || inviteData?.invitation_id || inviteData?.data?.id;

      if (inviteId) {
        return {
          success: true,
          invitation_id: inviteId,
          provider_id: providerId,
          public_identifier: publicIdentifier,
          personalized_message: personalizedMessage,
        };
      }

      // 2xx but no usable ID — fall through to chat
      console.warn(`[linkedin_message] Invite 2xx but no ID for ${publicIdentifier}, falling back to chat`);
    } else {
      const inviteStatus = inviteRes.status;
      await inviteRes.text(); // consume body

      if (inviteStatus !== 422 && inviteStatus !== 400) {
        // Non-recoverable error
        return { success: false, error: `Invite failed (${inviteStatus})` };
      }
      // 422 = already connected, 400 = message too long — fall through to chat
    }
  }

  // Step 5: Chat lookup fallback (max 3 pages × 20 = 60 chats)
  let resolvedChatId: string | undefined;
  for (let page = 1; page <= 3; page++) {
    const chatsRes = await unipileFetch(
      `${apiUrl}/api/v1/chats?account_id=${unipileAccountId}&limit=20&page=${page}`,
      { headers: { 'X-API-KEY': config.unipile.apiKey } }
    );

    if (!chatsRes.ok) break;

    const chatsData = await chatsRes.json().catch(() => null);
    const chats: any[] = chatsData?.items || [];

    const match = chats.find((c: any) => c.attendee_provider_id === providerId);
    if (match) {
      resolvedChatId = match.id;
      break;
    }

    if (chats.length < 20) break; // last page
  }

  if (resolvedChatId) {
    // Step 6: Send to found chat
    return sendToChat({
      apiUrl, unipileAccountId, chatId: resolvedChatId,
      personalizedMessage, use_inmail, providerId, publicIdentifier,
    });
  }

  // No chat found — create new chat
  const newChatRes = await unipileFetch(`${apiUrl}/api/v1/chats`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-KEY': config.unipile.apiKey },
    body: JSON.stringify({ account_id: unipileAccountId, text: personalizedMessage, attendees_ids: [providerId] }),
  });

  const newChatData = await newChatRes.json().catch(() => null);

  if (!newChatRes.ok) {
    return { success: false, error: newChatData?.message || newChatData?.error || `Chat create failed (${newChatRes.status})` };
  }

  return {
    success: true,
    message_id: newChatData?.id || null,
    chat_id: newChatData?.chat_id || null,
    provider_id: providerId,
    public_identifier: publicIdentifier,
    personalized_message: personalizedMessage,
  };
}

async function sendToChat(opts: {
  apiUrl: string;
  unipileAccountId: string;
  chatId: string;
  personalizedMessage: string;
  use_inmail?: boolean;
  providerId: string | null;
  publicIdentifier: string | null;
}): Promise<any> {
  const { apiUrl, chatId, personalizedMessage, use_inmail, providerId, publicIdentifier } = opts;

  const res = await unipileFetch(`${apiUrl}/api/v1/chats/${chatId}/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-KEY': config.unipile.apiKey },
    body: JSON.stringify({ text: personalizedMessage, type: use_inmail ? 'INMAIL' : 'MESSAGE' }),
  });

  const data = await res.json().catch(() => null);

  if (!res.ok) {
    return { success: false, error: data?.message || data?.error || `Chat send failed (${res.status})` };
  }

  return {
    success: true,
    message_id: data?.id || null,
    chat_id: chatId,
    provider_id: providerId,
    public_identifier: publicIdentifier,
    personalized_message: personalizedMessage,
  };
}
