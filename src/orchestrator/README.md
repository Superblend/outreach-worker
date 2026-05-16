# outreach-orchestrator

The orchestrator decides **which leads to start, when**, for sequences whose
owning client has opted into orchestrator mode. It replaces — for those
clients only — the daily `release-scheduled-batches` cron and the worker's
`scanner.ts` in-process polling.

Runs as a **separate Railway service** in the same project (`lavish-grace`),
single replica. Same repo, different `npm` start script.

## Why this exists

Today, daily lead release is split across two systems:

1. `release-scheduled-batches` (Supabase edge function, fired by `pg_cron` daily
   at 09:00 UTC) — turns pre-scheduled `unipile_batch_queue` rows into
   `unipile_sequence_executions`.
2. `scanner.ts` (15-second in-process loop inside the worker) — picks up due
   executions and enqueues BullMQ jobs.

This pre-scheduled, calendar-batched model is brittle. The orchestrator
replaces the *first* layer with a dynamic, on-demand model: count the leads
already started today for this sequence, pull more if there's room, enqueue,
done. No pre-population of `unipile_batch_queue`. No daily cron.

## What the orchestrator does NOT change

Hard invariants we keep, regardless of mode:

- **`check_and_increment_daily_limit` RPC** is the sole source of truth for
  per-Unipile-account daily caps. Worker still calls it at send time. Slot
  table never replaces or shadows this.
- **Account assignment stays in the worker.** Orchestrator creates executions
  with `assigned_*_account_id = NULL`; worker does sticky-per-lead rotation
  at dispatch time (`scanner.ts` → `worker-manager.ts`).
- **Priority cohort order (P1→P2→P3→P4)** — `cohortPriority()` from
  `scanner.ts` is preserved verbatim. Slot logic lives *inside* the cohort
  loop, not in place of it.
- **`trg_promote_lead_to_p2_after_first_touch`** trigger stays. Lead cohort
  transitions happen at the DB level, not in this service.
- **Active window / timezone enforcement** — orchestrator must validate
  `scheduled_start_time` / `scheduled_end_time` / `active_days` / `timezone`
  against the sequence's local clock *immediately before enqueue*, using the
  same `formatToParts()` pattern as `scanner.ts`.
- **Worker pacing, idempotency guard, heartbeat** — unchanged.

## Per-client opt-in: `clients.orchestrator_mode`

| Value | Behavior |
|---|---|
| `legacy` (default) | Current flow: `release-scheduled-batches` cron + worker scanner. Orchestrator does nothing. |
| `shadow` | Orchestrator runs full logic, writes intended decisions to `orchestrator_shadow_log`, **never enqueues**. Legacy paths still active. Used for shadow phase. |
| `orchestrator` | Orchestrator owns scheduling. Worker scanner skips this client's sequences. Edge dispatcher skips this client. |

The flag lives on `clients`, not `unipile_sequences` — when we flip a client,
all their sequences move together. This matches how testing in practice
happens: "let's try the new path with client X."

## Slot reservation table: `unipile_sequence_daily_leads`

```
unipile_sequence_id  uuid     NOT NULL
lead_id              uuid     NOT NULL
date                 date     NOT NULL DEFAULT CURRENT_DATE
slot_claimed_at      timestamptz NOT NULL DEFAULT now()
PRIMARY KEY (unipile_sequence_id, lead_id, date)
```

Semantics:

- **One slot per lead per sequence per day, channel-agnostic.** A multi-channel
  step (e.g., LinkedIn + email same day) counts as one slot.
- **Reservation, not a counter.** Inserted when the orchestrator decides to
  start this lead today. Existence of the row means "this lead has been
  started today for this sequence." It does not track sends.
- **Failure semantics**: if the execution later fails permanently, the slot is
  **not** released. A started lead counts even if it errors. Matches today's
  `daily_batch_size` behavior.
- **Same-day vs new-day rule**: keyed off calendar date in the sequence's
  timezone, NOT off `delay_unit`. If the next step's computed
  `next_execution_at` falls on the same local date as the lead's existing slot
  row, it's free. Different local date = new slot needed on that future day.
- **Retention**: 30 days. A small daily housekeeping job drops older rows.
  This is not a polling cron in the dispatch sense; it's pure retention.

## Architecture

```
                        ┌────────────────────────────────────┐
                        │ Supabase                           │
  ┌────────────────────►│ unipile_sequence_executions        │
  │                     │ unipile_sequence_daily_leads (new) │
  │                     │ clients.orchestrator_mode (new)    │
  │                     │ orchestrator_shadow_log (new)      │
  │                     └────────────────────────────────────┘
  │                              ▲          │
  │                              │          │ Realtime change events
  │   (5-min poll fallback)      │          ▼
  │                              │     ┌────────────────────────┐
  │                              │     │ outreach-orchestrator  │
  │                              │     │ (this service, single  │
  │                              │     │  Railway replica)      │
  │                              │     │                        │
  │                              │     │  scheduler:            │
  │                              │     │   - per-sequence loop  │
  │                              │     │   - cohort priority    │
  │                              │     │   - slot reservation   │
  │                              │     │   - window/timezone    │
  │                              │     └──────────┬─────────────┘
  │                              │                │
  │                              │                │ enqueue (or shadow log)
  │                              │                ▼
  │                              │           ┌──────────┐
  │                              └───────────┤  Redis   │
  │                                          │  BullMQ  │
  │                                          └────┬─────┘
  │                                               │ consume
  │                                               ▼
  │                                       ┌──────────────────┐
  │                                       │ outreach-worker  │
  │                                       │ (existing svc,   │
  │                                       │  N replicas)     │
  │                                       │                  │
  │   write step results, advance ◄──────┤  - acct assign   │
  │                                       │  - pacing locks  │
  │                                       │  - send (Unipile)│
  └───────────────────────────────────────┤  - idempotency   │
                                          │  - heartbeat     │
                                          └──────────────────┘
```

## File layout

| File | Role |
|---|---|
| `index.ts` | Service entry. Boots subscriber + fallback + scheduler. |
| `types.ts` | Shared types: events, decisions, mode, slot rows. |
| `mode-reader.ts` | Reads `clients.orchestrator_mode`, caches with short TTL. |
| `realtime-subscriber.ts` | Supabase Realtime on relevant tables, reconnect logic. |
| `poll-fallback.ts` | 5-min poll catching anything Realtime missed. |
| `scheduler.ts` | Core loop: cohort priority + slot reservation + window check + enqueue. |
| `slot-manager.ts` | `unipile_sequence_daily_leads` reserve / check / cleanup. |
| `shadow-logger.ts` | Writes decisions to `orchestrator_shadow_log` in shadow mode. |

## Migration phases

1. **Scaffold landed** (this commit) — code on `staging` branch, no migration applied, no service running, default `orchestrator_mode='legacy'` means zero behavior change.
2. **Migration applied to staging** — adds the table/column/log without changing behavior (still legacy by default).
3. **Orchestrator service deployed to staging Railway** — runs against staging Supabase, all clients still `legacy` so nothing happens yet.
4. **First staging client → `shadow`** — orchestrator computes decisions, writes shadow log, doesn't enqueue. Compare against legacy.
5. **Shadow gates pass** → flip same client to `orchestrator`. Worker scanner / edge dispatcher skip that client. Orchestrator drives.
6. **Repeat in prod**, canary-style, one client at a time.

## Open implementation questions (not blockers for scaffold)

- BullMQ topology for the orchestrator itself: a `outreach-orchestrator` queue
  for per-sequence wake-up jobs (delayed jobs), or just an in-memory min-heap
  of `(next_wake_at, sequence_id)`? In-memory is simpler but loses state on
  restart; BullMQ delayed jobs are durable.
- What happens when a client is flipped from `orchestrator` → `legacy`
  (rollback)? Existing in-flight executions are fine (they were already
  enqueued, worker just consumes). But the orchestrator should stop scheduling
  new ones for that client immediately. Cache invalidation needs care.
- `unipile_batch_queue` legacy rows for an `orchestrator`-mode client:
  ignored. Confirmed Q1 = option (c).
