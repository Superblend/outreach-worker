import { withTimeout, CALL_TIMEOUT_MS } from './with-timeout';

const RETRYABLE_STATUS_CODES = [429, 503];
const MAX_RETRIES = 3;
const BASE_DELAY_MS = 2000;

export async function unipileFetch(url: string, options?: RequestInit): Promise<Response> {
  // Build a short tag from the URL path (drop host + query string) for timeout logs.
  const tag = `unipile:${url.replace(/^https?:\/\/[^/]+/, '').replace(/\?.*$/, '').slice(0, 80)}`;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const response = await withTimeout(fetch(url, options), CALL_TIMEOUT_MS, tag);
    if (!RETRYABLE_STATUS_CODES.includes(response.status) || attempt === MAX_RETRIES) return response;
    const retryAfter = response.headers.get('Retry-After');
    const delayMs = retryAfter ? parseInt(retryAfter, 10) * 1000 : BASE_DELAY_MS * Math.pow(2, attempt);
    console.warn(`Unipile ${response.status}, retry in ${(delayMs / 1000).toFixed(1)}s (${attempt + 1}/${MAX_RETRIES})`);
    await response.text();
    await new Promise(r => setTimeout(r, delayMs));
  }
  throw new Error('Unreachable');
}
