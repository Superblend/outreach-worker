import { supabase } from '../supabase';
import { config } from '../config';
import { unipileFetch } from './unipile-fetch';
import { normalizeAndReplace } from './variable-replace';

interface SendLinkedInInvitationParams {
  account_id: string;
  lead: any;
  message?: string;
}

function extractLinkedInIdentifier(lead: any): string | null {
  const url = lead.linkedin_url || lead.linkedin || '';
  if (!url) return null;
  const match = url.match(/linkedin\.com\/in\/([^/?#]+)/i);
  if (match) return match[1];
  // Bare identifier (no URL)
  if (!url.includes('/') && !url.includes('.')) return url;
  return null;
}

export async function sendLinkedInInvitation(params: SendLinkedInInvitationParams): Promise<any> {
  const { account_id, lead } = params;
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

  const publicIdentifier = extractLinkedInIdentifier(lead);
  if (!publicIdentifier) {
    return { success: false, error: 'Missing required LinkedIn URL on lead' };
  }

  // Fetch profile to get provider_id
  const profileRes = await unipileFetch(
    `${apiUrl}/api/v1/users/${publicIdentifier}?account_id=${unipileAccountId}`,
    {
      headers: { 'X-API-KEY': config.unipile.apiKey },
    }
  );

  const profileData = await profileRes.json().catch(() => null);

  if (!profileRes.ok) {
    return { success: false, error: profileData?.error || profileData?.message || `Profile fetch failed: ${profileRes.status}` };
  }

  const providerId = profileData?.provider_id || null;
  if (!providerId) {
    return { success: false, error: 'Could not get provider_id from LinkedIn profile' };
  }

  const resolvedProfileUrl = lead.linkedin || lead.linkedin_url || null;
  const personalizedMessage = params.message ? normalizeAndReplace(params.message, lead) : '';

  // Send invitation
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

  const inviteData = await inviteRes.json().catch(() => null);

  if (!inviteRes.ok) {
    const errorMsg = inviteData?.error || inviteData?.message || inviteRes.statusText;
    // Return partial data even on failure so webhook can match if a delayed acceptance arrives
    return {
      success: false,
      error: errorMsg,
      provider_id: providerId,
      public_identifier: publicIdentifier,
      resolved_profile_url: resolvedProfileUrl,
      sent_from_unipile_account: unipileAccountId,
    };
  }

  const invitationId = inviteData?.id || inviteData?.invitation_id || null;

  // Try to fetch chat_id (non-critical)
  let chatId: string | undefined;
  try {
    const chatsRes = await unipileFetch(
      `${apiUrl}/api/v1/chats?account_id=${unipileAccountId}&limit=20`,
      { headers: { 'X-API-KEY': config.unipile.apiKey } }
    );
    const chatsData = await chatsRes.json().catch(() => null);
    const chats = chatsData?.items || [];
    const matchedChat = chats.find((c: any) => c.attendee_provider_id === providerId);
    if (matchedChat) chatId = matchedChat.id;
  } catch {
    // Non-critical, ignore
  }

  return {
    success: true,
    invitation_id: invitationId,
    chat_id: chatId || null,
    provider_id: providerId,
    public_identifier: publicIdentifier,
    resolved_profile_url: resolvedProfileUrl,
    sent_from_unipile_account: unipileAccountId,
    personalized_message: personalizedMessage,
  };
}
