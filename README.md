# Outreach Worker — BullMQ + Redis

Replaces pg_cron-based outreach dispatching with Redis-backed job queues.

## Quick Start

1. **Push to a NEW GitHub repo** (not your main frontend repo)
2. **In Railway**: Click "+ New" → "GitHub Repo" → select this repo
3. **Set environment variables** on the Railway service:

| Variable | Value |
|----------|-------|
| `REDIS_URL` | Use "Add Reference" → select your Railway Redis instance |
| `SUPABASE_URL` | `https://nyzpdrovlsynffykhsgg.supabase.co` |
| `SUPABASE_SERVICE_ROLE_KEY` | From Supabase Dashboard → Settings → API |
| `SUPABASE_ANON_KEY` | From Supabase Dashboard → Settings → API |

4. **Deploy** — Railway auto-builds via Dockerfile

## How it works

- **Scanner** (every 60s): Queries DB for due executions where `use_bullmq=true`, enqueues BullMQ jobs
- **Execution Worker**: Processes jobs by calling `unipile-execute-sequence-step` Edge Function via HTTP
- **Batch Worker**: Processes batch jobs by calling `unipile-process-batch-queue` Edge Function via HTTP
- **Recovery** (every 5min): Fixes stranded delay-step executions
- **Bull Board**: Dashboard at `/admin/queues` for monitoring

## Migration

1. Deploy worker → it starts scanning but finds nothing (no sequences have `use_bullmq=true`)
2. Enable on test sequences: `UPDATE unipile_sequences SET use_bullmq = true WHERE id IN (...)`
3. Monitor Bull Board + Edge Function logs for 48 hours
4. Ramp to 100%: `UPDATE unipile_sequences SET use_bullmq = true`
5. Disable pg_cron scheduler/batch dispatcher jobs

## Local Development

```bash
npm install
cp .env.example .env  # Fill in values
npm run dev
```
