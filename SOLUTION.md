# SOLUTION

## What was broken, and why

**1. Duplicate events / inflated stats.** `Ingest()` checked `EventExists()` then
called `InsertEvent()` as two separate steps, and `events.event_id` only had a
plain index — not a `UNIQUE` constraint. The provider redelivers, sometimes
near-simultaneously, so two deliveries of the same `event_id` could both pass
the exists-check before either had written a row: classic check-then-act
race. Both rows land, `account_stats` gets incremented twice.

**2. Stats drift.** Separately, `stats.Cache.Record` mutated the shared map
**without taking the mutex** — only `Get` locked it. Concurrent deliveries
raced on the same account's in-memory counters, so `/accounts/{id}/stats`
could drift independently of the Postgres numbers even with dedup fixed.

**3. Recordings never marked processed, silently.** The background goroutine
was handed `r.Context()`. `net/http` cancels a request's context the instant
the handler returns, and the handler returns immediately by design (that's
the point of doing this async). So by the time the goroutine's 50ms "work"
finished, its context was already canceled, `MarkRecordingProcessed` failed,
and the error was dropped by a bare `// TODO: handle`.

**4. Work disappears on deploy.** `srv.Shutdown()` only waits for HTTP
handlers still executing. Since `Ingest` never blocks on the recording
goroutine, the handler is long gone by the time `SIGTERM` arrives — nothing
tracked or waited for that goroutine, so it was abandoned mid-run on every
deploy.

## Dedup strategy

I made `events.event_id` genuinely `UNIQUE` (migration `002`) and replaced
the check-then-insert sequence with one method, `Store.IngestEvent`, that
does `INSERT INTO events ... ON CONFLICT (event_id) DO NOTHING RETURNING id`
and, only if a row came back, upserts the call and increments
`account_stats` — all inside a single Postgres transaction. "Have we seen
this before?" and "record it" become one atomic operation with no window
between them, so no number of concurrent redeliveries can double-insert or
double-count.

I considered a Redis `SETNX` as the dedup gate instead. I rejected it as the
*source of truth*: Redis can lose keys (eviction, restart with no
persistence guarantee) independently of what's in Postgres, which would
silently let a duplicate back through — reopening exactly the bug this is
supposed to fix. A unique constraint enforced in the same transaction as the
write has no such failure mode. Redis is a fine *accelerator* in front of
Postgres (see below), just not the authority.

## At 10,000 webhooks/sec

- Put a Redis `SET event_id NX EX <ttl>` in front of the Postgres path as a
  cheap pre-filter: most redeliveries get rejected before touching the
  database. Postgres's unique constraint stays as the correctness backstop —
  a false negative from Redis just costs one wasted, safely-ignored insert,
  never a duplicate.
- Move recording processing off ad hoc goroutines onto something durable —
  a Postgres outbox table or a real queue — so it survives process restarts
  instead of relying on graceful shutdown always going smoothly.
- Batch the `account_stats` increments (e.g. accumulate in Redis and flush
  periodically) rather than one row-level `UPDATE` per event; that row is
  the hottest write in the system per account and will become a lock
  bottleneck well before 10k/sec.
- Horizontally scale the HTTP tier behind a load balancer; the ingest path
  is already stateless per request given the fixes above.

## What I'd do next if I had more time

- Add an index on `calls.account_id` / a covering index for the stats
  read path if `GET /accounts/{id}/stats` ever needs to fall back to
  Postgres under cache misses.
- Reconcile the in-memory cache against `account_stats` periodically
  (or on read) to bound how far they can drift if the process restarts
  between the DB commit and the `cache.Record` call — right now those two
  are not atomic with each other, only the DB-side writes are.
