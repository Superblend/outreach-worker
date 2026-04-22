# Patch Notes — Queue Poisoning Fix

## Problem

After the PTTL-based pacing fix, stale and non-send jobs were consuming pacing slots and `worker.rateLimit` pauses on per-account LinkedIn queues, stalling real invite sending:

1. **Completed executions re-dequeued** — finished executions were picked up again, acquired a LinkedIn pacing slot, then discarded after noticing they were done. The slot and the 45–90s `worker.rateLimit` pause were wasted.
2. **Delay/conditional steps consuming pacing** — `delay` and `conditional` steps routed through a LinkedIn account queue were unconditionally acquiring LinkedIn pacing slots before their logic ran, burning slots that should be reserved for outbound API calls.
3. **Pacing-defer jobId churn** — the pacing-deferred requeue used `Date.now()` in the jobId, so each requeue created a new delayed job instead of deduplicating. Stale jobs accumulated and starved the queue.

## Changes

### `src/workers/execution.worker.ts`

- **`WORKER_SESSION_ID`** — per-process constant (rotates on restart) used in stable pacing jobIds.
- **`PACEABLE_LINKEDIN_STEP_TYPES`** — set of step types that actually call LinkedIn APIs. Only these acquire a pacing slot. `delay`, `conditional`, and other internal steps pass through without touching Redis pacing state.
- **Freshness check** (runs first, before pacing): reloads `status` + `current_step_id` from DB and checks for an existing `success` row in `unipile_step_results`. If the job is stale, logs `🪦 [stale-job]` and returns immediately — no pacing consumed, no `worker.rateLimit`, no requeue.
- **LinkedIn pacing gate**: `if (channel === 'linkedin' && PACEABLE_LINKEDIN_STEP_TYPES.has(stepType ?? ''))`
- **Email pacing gate**: `if (channel === 'email' && stepType === 'email')`
- **Stable pacing jobId**: `exec-${execution_id}-${stepId}-${WORKER_SESSION_ID}-pacing` — deduplicates repeated requeues for the same execution-step within a worker session.
- **`ExecutionJobData`** — added optional `step_id` and `step_type` fields.

### `src/scanner.ts`

- Added `step_id: exec.current_step_id` and `step_type: stepType` to the BullMQ job data payload. These feed the freshness check and pacing gate in the worker without extra DB queries.

### `scripts/recover-poisoned-queues.ts` (new)

One-time recovery script that scans all `outreach-linkedin-*` and `outreach-email-client-*` queues for delayed jobs and removes any that are stale (execution not running, step advanced, or step already succeeded). Only touches delayed jobs.

## What was NOT changed

- `PACING_LUA` / `EMAIL_PACING_LUA` — unchanged
- 45–90s LinkedIn / 2–3s email pacing windows — unchanged
- `check_and_increment_daily_limit` RPC behavior — unchanged
- P1–P6 cohort priority ordering — unchanged
- PTTL-based `requeueDelay`, `Math.max(waitMs, 1000)` floor, 0–500ms jitter — unchanged
- `worker.rateLimit(requeueDelay)` + `throw Worker.RateLimitError()` — unchanged

## Deploy & Recovery Sequence

1. **Deploy** the updated worker to Railway (auto-deploys on push to `main`).
2. **Wait** for the new replica to come up and start logging.
3. **Run the recovery script** against the production Redis + Supabase:
   ```bash
   REDIS_URL=... SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... \
     npx ts-node scripts/recover-poisoned-queues.ts
   ```
4. **Restart worker replicas** in Railway so they pick up the now-clean queues with fresh `WORKER_SESSION_ID` values.
5. **Monitor** Railway logs for:
   - `🪦 [stale-job]` lines appearing then tapering off
   - No more `[linkedin-pacing]` lines for `step_type=delay`
   - `⏳ [linkedin-pacing] requeued in ~60s` only for genuine outbound steps
6. **Verify** in Supabase: gaps between `unipile_step_results.executed_at` rows for invite steps should be 45–90s consistently.
