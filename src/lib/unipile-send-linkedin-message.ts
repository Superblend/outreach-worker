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
}

function extractLinkedInIdentifier(lead: any): string | null {
  const url = lead.linkedin_url || lead.linkedin || '';
  if (!url) return null;
  const match = url.match(/linkedin\.com\/in\/([^/?#]+)/i);
  if (match) return match[1];
  if (!url.includes('/') && !url.includes('.')) return url;
  return null;
}

export async function sendLinkedInMessage(params: SendLinkedInMessageParams): Promise<any> {
  const { account_id, lead, use_inmail, chat_id } = params;
  const apiUrl = config.unipile.apiUrl;

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
  const personalizedMessage = normalizeAndReplace(params.message, lead);

  // If chat_id provided, send directly
  if (chat_id) {
    const res = await unipileFetch(`${apiUrl}/api/v1/chats/${chat_id}/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-KEY': config.unipile.apiKey,
      },
      body: JSON.stringify({
        text: personalizedMessage,
        type: use_inmail ? 'INMAIL' : 'MESSAGE',
      }),
    });

    const data = await res.json().catch(() => null);

    if (!res.ok) {
      return { success: false, error: data?.error || data?.message || res.statusText };
    }

    if (!data?.id) {
      return { success: false, error: 'Message sent but no id returned' };
    }

    return {
      success: true,
      message_id: data.id,
      chat_id,
      provider_id: null,
      public_identifier: null,
      personalized_message: personalizedMessage,
      raw: data,
    };
  }

  // No chat_id: resolve via LinkedIn identifier
  const publicIdentifier = extractLinkedInIdentifier(lead);
  if (!publicIdentifier) {
    return { success: false, error: 'Missing required LinkedIn URL on lead' };
  }

  // Fetch profile for provider_id
  const profileRes = await unipileFetch(
    `${apiUrl}/api/v1/users/${publicIdentifier}?account_id=${unipileAccountId}`,
    { headers: { 'X-API-KEY': config.unipile.apiKey } }
  );
  const profileData = await profileRes.json().catch(() => null);

  if (!profileRes.ok) {
    return { success: false, error: profileData?.error || `Profile fetch failed: ${profileRes.status}` };
  }

  const providerId = profileData?.id || profileData?.provider_id;
  if (!providerId) {
    return { success: false, error: 'Could not get provider_id from LinkedIn profile' };
  }

  // Try invite endpoint first
  const inviteRes = await unipileFetch(`${apiUrl}/api/v1/users/invite`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-KEY': config.unipile.apiKey,
    },
    body: JSON.stringify({
      account_id: unipileAccountId,
      provider_id: providerId,
      message: personalizedMessage,
    }),
  });

  if (inviteRes.ok) {
    const inviteData = await inviteRes.json().catch(() => null);

    if (!inviteData?.id) {
      return { success: false, error: 'Invite sent but no id returned' };
    }

    return {
      success: true,
      invitation_id: inviteData.id,
      provider_id: providerId,
      public_identifier: publicIdentifier,
      personalized_message: personalizedMessage,
      raw: inviteData,
    };
  }

  // 422 = already connected, 400 = message too long — fall back to chat
  const inviteStatus = inviteRes.status;
  if (inviteStatus === 422 || inviteStatus === 400) {
    await inviteRes.text(); // consume body

    // Search existing chats by page (max 50 chats = 3 pages of 20)
    let resolvedChatId: string | undefined;
    const maxChats = 50;
    const pageSize = 20;
    let page = 1;
    let fetched = 0;

    while (fetched < maxChats) {
      const url = `${apiUrl}/api/v1/chats?account_id=${unipileAccountId}&page=${page}&limit=${pageSize}`;
      const chatsRes = await unipileFetch(url, { headers: { 'X-API-KEY': config.unipile.apiKey } });
      const chatsData = await chatsRes.json().catch(() => null);
      const chats: any[] = chatsData?.items || chatsData?.chats || [];

      const match = chats.find((c: any) => c.attendee_provider_id === providerId);

      if (match) {
        resolvedChatId = match.id;
        break;
      }

      fetched += chats.length;
      if (chats.length < pageSize) break;
      page++;
    }

    if (resolvedChatId) {
      const msgRes = await unipileFetch(`${apiUrl}/api/v1/chats/${resolvedChatId}/messages`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-KEY': config.unipile.apiKey,
        },
        body: JSON.stringify({
          text: personalizedMessage,
          type: use_inmail ? 'INMAIL' : 'MESSAGE',
        }),
      });

      const msgData = await msgRes.json().catch(() => null);

      if (!msgRes.ok) {
        return { success: false, error: msgData?.error || msgData?.message || msgRes.statusText };
      }

      if (!msgData?.id) {
        return { success: false, error: 'Message sent but no id returned' };
      }

      return {
        success: true,
        message_id: msgData.id,
        chat_id: resolvedChatId,
        provider_id: providerId,
        public_identifier: publicIdentifier,
        personalized_message: personalizedMessage,
        raw: msgData,
      };
    }

    // No chat found: create new chat
    const newChatRes = await unipileFetch(`${apiUrl}/api/v1/chats`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-KEY': config.unipile.apiKey,
      },
      body: JSON.stringify({
        account_id: unipileAccountId,
        text: personalizedMessage,
        attendees_ids: [providerId],
      }),
    });

    const newChatData = await newChatRes.json().catch(() => null);

    if (!newChatRes.ok) {
      return { success: false, error: newChatData?.error || newChatData?.message || newChatRes.statusText };
    }

    if (!newChatData?.id) {
      return { success: false, error: 'Chat created but no id returned' };
    }

    return {
      success: true,
      message_id: newChatData.id,
      chat_id: newChatData.chat_id || newChatData.id,
      provider_id: providerId,
      public_identifier: publicIdentifier,
      personalized_message: personalizedMessage,
      raw: newChatData,
    };
  }

  // Other error
  const errData = await inviteRes.json().catch(() => null);
  return { success: false, error: errData?.error || errData?.message || inviteRes.statusText };
}
