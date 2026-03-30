import { supabase } from '../supabase';
import { config } from '../config';
import { unipileFetch } from './unipile-fetch';

interface VisitProfileParams {
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

export async function visitProfile(params: VisitProfileParams): Promise<any> {
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

  const res = await unipileFetch(
    `${apiUrl}/api/v1/users/${publicIdentifier}?account_id=${unipileAccountId}&notify=true`,
    { headers: { 'X-API-KEY': config.unipile.apiKey } }
  );

  const data = await res.json().catch(() => null);

  if (!res.ok) {
    return { success: false, error: data?.error || data?.message || `Profile visit failed: ${res.status}` };
  }

  return {
    success: true,
    provider_id: data?.id || data?.provider_id,
    profile_name: data?.full_name || data?.name || publicIdentifier,
  };
}
