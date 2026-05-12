import { createClient } from '@supabase/supabase-js';
import { config } from './config';
import { withTimeout } from './lib/with-timeout';

export const supabase = createClient(
  config.supabase.url,
  config.supabase.serviceRoleKey
);

/**
 * Call an Edge Function via HTTP (same as pg_cron does).
 * Uses the anon key for authorization.
 */
export async function invokeEdgeFunction(
  functionName: string,
  body: Record<string, unknown>
): Promise<{ data: any; error: any }> {
  const url = `${config.supabase.url}/functions/v1/${functionName}`;
  
  try {
    const res = await withTimeout(
      fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${config.supabase.anonKey}`,
        },
        body: JSON.stringify(body),
      }),
      30_000,
      `supabase:edge:${functionName}`,
    );

    const data = await res.json().catch(() => null);

    if (!res.ok) {
      return { data: null, error: { status: res.status, message: data?.error || res.statusText } };
    }

    return { data, error: null };
  } catch (err: any) {
    return { data: null, error: { message: err.message } };
  }
}
