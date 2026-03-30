import { supabase } from '../supabase';
import { config } from '../config';
import { unipileFetch } from './unipile-fetch';

interface CheckConnectionParams {
  account_id: string;
  lead: any;
}

function extractLinkedInIdentifier(lead: any): string | null {
  const url = lead.linkedin_url || lead.linkedin || '';
  if (!url) return null;
  const match = url.match(/linkedin\.com\/in\/([^/?#]+)/i);
  if (match) return match[1];
  if (!url.includes('/') && !url.includes('.')) return url;
  return null;
}

export async function checkConnection(params: CheckConnectionParams): Promise<any> {
  const { account_id, lead } = params;
  const apiUrl = config.unipile.apiUrl;

  const { data: accountRow, error: accountError } = await supabase
    .from('unipile_accounts')
    .select('account_id, status')
    .eq('id', account_id)
    .single();

  if (accountError || !accountRow) {
    return { success: false, connected: false, error: `Account not found: ${account_id}` };
  }

  if (accountRow.status !== 'active') {
    return { success: false, connected: false, error: `Account not active` };
  }

  const unipileAccountId = accountRow.account_id;

  const publicIdentifier = extractLinkedInIdentifier(lead);
  if (!publicIdentifier) {
    return { success: true, connected: false, reason: 'no_linkedin_url' };
  }

  // Fetch profile to check connection status
  const profileRes = await unipileFetch(
    `${apiUrl}/api/v1/users/${publicIdentifier}?account_id=${unipileAccountId}`,
    { headers: { 'X-API-KEY': config.unipile.apiKey } }
  );

  const profileData = await profileRes.json().catch(() => null);

  if (!profileRes.ok) {
    return { success: false, connected: false, error: profileData?.error || `Profile fetch failed: ${profileRes.status}` };
  }

  const providerId = profileData?.id || profileData?.provider_id;
  const isConnected = profileData?.is_relationship === true || profileData?.network_distance === 'FIRST_DEGREE';

  if (isConnected) {
    // Quick single-page chat lookup to find chat_id
    let chatId: string | undefined;
    try {
      const chatsRes = await unipileFetch(
        `${apiUrl}/api/v1/chats?account_id=${unipileAccountId}&limit=100`,
        { headers: { 'X-API-KEY': config.unipile.apiKey } }
      );
      const chatsData = await chatsRes.json().catch(() => null);
      const chats = chatsData?.items || chatsData?.chats || [];
      const match = chats.find((c: any) =>
        c.attendees?.some((a: any) => a.provider_id === providerId)
      );
      if (match) chatId = match.id;
    } catch {
      // Non-critical
    }

    return {
      success: true,
      connected: true,
      chat_id: chatId,
      provider_id: providerId,
      connection_method: 'profile_check',
    };
  }

  // NOT connected from profile — full chat pagination fallback
  let chatId: string | undefined;
  let cursor: string | undefined;

  try {
    while (true) {
      const url = `${apiUrl}/api/v1/chats?account_id=${unipileAccountId}&limit=20${cursor ? `&cursor=${cursor}` : ''}`;
      const chatsRes = await unipileFetch(url, { headers: { 'X-API-KEY': config.unipile.apiKey } });
      const chatsData = await chatsRes.json().catch(() => null);
      const chats = chatsData?.items || chatsData?.chats || [];

      const match = chats.find((c: any) =>
        c.attendees?.some((a: any) => a.provider_id === providerId)
      );

      if (match) {
        chatId = match.id;
        break;
      }

      cursor = chatsData?.cursor || chatsData?.next_cursor;
      if (!cursor || chats.length === 0) break;
    }
  } catch {
    // Non-critical
  }

  return {
    success: true,
    connected: !!chatId,
    chat_id: chatId,
    provider_id: providerId,
    connection_method: chatId ? 'chat_fallback' : 'not_connected',
  };
}
