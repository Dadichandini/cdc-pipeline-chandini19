#### CDC Pipeline: Change Events to Current State + Audit History

#### A Python + SQLite pipeline that turns insert/update/delete events into a current-state table while keeping a full, auditable history. It handles duplicate and out-of-order events, supports safe reruns, traces every rejected record to its source line, and proves correctness with reconciliation checks and 14 automated tests.

#### Highlights:processes 109k events in about 2.4 s | idempotent reruns | soft deletes | 7 reconciliation checks | reproducible synthetic data (fixed seed)





# CDC Pipeline: change events to current state + audit history

Processes insert/update/delete events (JSONL) into a **current-state table** while keeping an
**append-only history**. Handles duplicate and out-of-order events; reruns are safe.
Python 3.9+, SQLite (stdlib), pytest. No external services, no secrets.

## Quick start

```bash
pip install -r requirements.txt
python -m cdc.generate --orders 60 --seed 42 --out data/sample          # small sample (committed)
python -m cdc.generate --orders 40000 --seed 7 --out data/big           # \\\~109k events
python -m cdc.pipeline --db out/cdc.db --customers data/sample/customers.csv --input data/sample/events.jsonl
python -m cdc.reconcile --db out/cdc.db --expected data/sample/expected.json
python -m pytest -q
```

## Architecture

```
events.jsonl --> parse --> validate --+--> rejects (reason, source\\\_file, line\\\_no, raw)
                                      |
                                      +--> dedupe (event\\\_id in history?) --> duplicate (counted, skipped)
                                      |
                                      +--> orders\\\_history (always, applied=0/1)
                                      +--> orders\\\_current (only if event is newer)
                      every run --> run\\\_log (counts, status, failure\\\_reason)
```

## Data dictionary

|Table|Key|Purpose|
|-|-|-|
|`customers`|`customer\\\_id`|Reference data for FK validation|
|`orders\\\_current`|`order\\\_id`|Latest state. `is\\\_deleted` = soft delete. `last\\\_event\\\_ts/id` = version|
|`orders\\\_history`|`event\\\_id`|Every accepted event, `applied` 1 = changed state, 0 = stale (late)|
|`rejects`|`(source\\\_file, line\\\_no)`|Invalid rows with `reason` and raw text|
|`run\\\_log`|`run\\\_id`|start/end, input/applied/stale/duplicate/rejected counts, status, failure reason|

Event fields: `event\\\_id, order\\\_id, op (I/U/D), event\\\_ts (ISO-8601 with timezone), customer\\\_id, amount (>=0), status (new|paid|shipped|cancelled)`. `D` events need only the first four.
Naming: snake\_case, ISO-8601 UTC timestamps, deterministic IDs (`E…`, `O…`, `C…`), `run\\\_id` and `source\\\_file` on audit rows.

## Rules and assumptions

* **Version = `(event\\\_ts, event\\\_id)`**. An event updates current state only if strictly newer; otherwise it is `stale` (kept in history, not applied). Ties broken by `event\\\_id`.
* **Events are full snapshots** (I/U carry all fields), so applying only the newest is correct regardless of arrival order.
* **Duplicate** = `event\\\_id` already in history. Skipped, counted.
* **Delete** = soft delete. Payload fields are kept; a newer insert/update un-deletes. A delete that arrives before any insert creates a tombstone with null payload (full data stays in history).
* **Rejected**: malformed JSON, missing/blank required field, bad op/timestamp (no timezone), unknown fields (schema change), null/negative/non-finite amount, bad status, customer not in `customers`.

## Idempotency, retry, backfill

* PKs on `event\\\_id`, `order\\\_id`, `(source\\\_file, line\\\_no)` + upserts: rerunning the same file changes nothing (`applied=0`, all duplicates; rejects not re-inserted).
* Commits happen per batch. On failure the open batch is rolled back, `run\\\_log.status='failed'` with reason; **rerunning resumes** because committed events are deduped.
* Backfill: feed older files in any order. Version rule guarantees the same final state.

## Reconciliation (`python -m cdc.reconcile`)

`input = applied + stale + duplicate + rejected` per run; history rows = applied+stale; current row = newest history event per order; no orphan FKs; final state equals the generator's independent expected state; rejects = injected invalid rows. Expected labels live in `expected.json`, separate from input data.
Sample output: `sample\\\_output/` (tables as CSV + `reconciliation.txt`).

Example report:

```sql
SELECT status, COUNT(\\\*) orders, ROUND(SUM(amount),2) revenue
FROM orders\\\_current WHERE is\\\_deleted=0 GROUP BY status;
```

## Performance (109,331 events,Windows laptop,python 3.13)

|Batch size|Time|
|-|-|
|1 (commit per event)|119.0 s|
|**5000 (default)**|**2.4 s** (\~46k events/s)|
|Rerun, all duplicates|0.9 s|

**Optimization applied:** batched commits (about 48x faster). Next candidates: `executemany` with bulk dedupe via a temp table, in-memory cache of current versions, WAL mode.

## Design trade-offs and limitations

* SQLite for zero setup; the same SQL ports to Postgres (`ON CONFLICT` is identical).
* Row-at-a-time lookups keep logic simple and obviously correct; bulk set-based merge is faster but harder to audit.
* Dedupe is by `event\\\_id` only; a replay with the same id and different content is treated as a duplicate.
* A file is processed in arrival order, single process; no concurrency control or streaming source.
* Rows that fail validation are never auto-retried; fix the source and resubmit.
* Next steps: schema registry for versioned schemas, dead-letter replay command, Postgres target.

## Tests (`tests/`, 14)

happy path, empty batch, malformed JSON, missing field, nulls, invalid FK, schema change, duplicate, out-of-order, delete + late update, timezone normalization, rerun idempotency, partial failure + retry, reconciliation identity.

