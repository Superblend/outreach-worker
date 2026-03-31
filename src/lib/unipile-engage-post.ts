import { supabase } from '../supabase';
import { config } from '../config';
import { unipileFetch } from './unipile-fetch';

interface EngagePostParams {
  account_id: string;
  lead: any;
  action_type?: string;
  comment_text?: string;
}

function extractLinkedInIdentifier(lead: any): string | null {
  const url = lead.linkedin || lead.linkedin_url || '';
  if (!url) return null;
  const match = url.match(/linkedin\.com\/in\/([^/?]+)/);
  if (match && match[1]) return match[1].replace(/\/$/, '');
  return null;
}

export async function engagePost(params: EngagePostParams): Promise<any> {
  const { account_id, lead, comment_text } = params;
  const action_type = params.action_type || 'like';
  const apiUrl = config.unipile.apiUrl;

  // Resolve Unipile account
  const { data: accountRow, error: accountError } = await supabase
    .from('unipile_accounts')
    .select('account_id, status')
    .eq('id', account_id)
    .single();

  if (accountError || !accountRow) {
    return { success: false, error: `Account not found: ${account_id}` };
  }

  if (accountRow.status !== 'active') {
    return { success: false, error: `Account not active: ${accountRow.status}` };
  }

  const unipileAccountId = accountRow.account_id;

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
    return { success: false, error: `Profile fetch failed (${profileRes.status}): ${errText}` };
  }

  const profileData = await profileRes.json().catch(() => null);
  const providerId: string | null = profileData?.provider_id || null;

  if (!providerId) {
    return { success: false, error: 'Could not get provider_id from LinkedIn profile' };
  }

  // Step 2: Fetch recent posts
  const postsRes = await unipileFetch(
    `${apiUrl}/api/v1/users/${providerId}/posts?account_id=${unipileAccountId}&limit=5`,
    { headers: { 'X-API-KEY': config.unipile.apiKey } }
  );

  if (!postsRes.ok) {
    // 4xx means user may have no posts — skip gracefully
    return {
      success: true,
      skipped: true,
      reason: 'no_posts',
      provider_id: providerId,
    };
  }

  const postsData = await postsRes.json().catch(() => null);
  const posts: any[] = postsData?.items || postsData || [];

  if (!posts.length) {
    return {
      success: true,
      skipped: true,
      reason: 'no_posts',
      provider_id: providerId,
    };
  }

  const targetPost = posts[0];
  const postSocialId: string = targetPost.social_id || targetPost.id;
  const postPreview: string = (targetPost.text || '').substring(0, 100);

  // Step 3: Engage
  if (action_type === 'like') {
    const reactionRes = await unipileFetch(`${apiUrl}/api/v1/posts/reaction`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-KEY': config.unipile.apiKey },
      body: JSON.stringify({ account_id: unipileAccountId, post_id: postSocialId, reaction_type: 'like' }),
    });

    if (!reactionRes.ok) {
      const errText = await reactionRes.text().catch(() => '');
      return { success: false, error: `Like failed (${reactionRes.status}): ${errText}` };
    }

    return {
      success: true,
      action: 'like',
      post_id: postSocialId,
      post_preview: postPreview,
      provider_id: providerId,
    };
  }

  if (action_type === 'comment') {
    if (!comment_text) {
      return { success: false, error: 'comment_text is required for comment action' };
    }

    const commentRes = await unipileFetch(`${apiUrl}/api/v1/posts/${postSocialId}/comments`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-KEY': config.unipile.apiKey },
      body: JSON.stringify({ account_id: unipileAccountId, text: comment_text }),
    });

    if (!commentRes.ok) {
      const errText = await commentRes.text().catch(() => '');
      return { success: false, error: `Comment failed (${commentRes.status}): ${errText}` };
    }

    return {
      success: true,
      action: 'comment',
      post_id: postSocialId,
      post_preview: postPreview,
      comment_text,
      provider_id: providerId,
    };
  }

  return { success: false, error: `Unknown action_type: ${action_type}` };
}
