import { config } from '../config';

const RETRYABLE_STATUS_CODES = [429, 503];
const MAX_RETRIES = 3;
const BASE_DELAY_MS = 2000;

export async function unipileFetch(url: string, options?: RequestInit): Promise<Response> {
  const method = (options?.method || 'GET').toUpperCase();
  if (config.dryRun && method !== 'GET') {
    const fakeId = `dry_run_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    console.warn(`[DRY_RUN] ${method} ${url} short-circuited; fake id=${fakeId}`);
    return new Response(
      JSON.stringify({ id: fakeId, dry_run: true }),
      { status: 200, headers: { 'content-type': 'application/json' } },
    );
  }

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const response = await fetch(url, options);
    if (!RETRYABLE_STATUS_CODES.includes(response.status) || attempt === MAX_RETRIES) return response;
    const retryAfter = response.headers.get('Retry-After');
    const delayMs = retryAfter ? parseInt(retryAfter, 10) * 1000 : BASE_DELAY_MS * Math.pow(2, attempt);
    console.warn(`Unipile ${response.status}, retry in ${(delayMs / 1000).toFixed(1)}s (${attempt + 1}/${MAX_RETRIES})`);
    await response.text();
    await new Promise(r => setTimeout(r, delayMs));
  }
  throw new Error('Unreachable');
}
