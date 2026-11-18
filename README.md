# Relay

Relay is a Go background job queue backed by PostgreSQL 17. The worker executable runs the PostgreSQL queue engine with bounded concurrency, lease renewal, retries, and crash recovery. The authenticated HTTP API submits jobs and exposes their lifecycle.

## Docker quick start

From a fresh clone, install Docker with Compose and generate a local API key. Run these commands in the same shell:

```sh
cp -n .env.example .env
export RELAY_API_KEY="$(openssl rand -hex 32)"
docker compose up --build -d --wait
curl -fsS http://127.0.0.1:8080/readyz
curl -fsS -H "Authorization: Bearer $RELAY_API_KEY" -H 'Content-Type: application/json' \
  -d '{"type":"demo.sleep","payload":{}}' http://127.0.0.1:8080/v1/jobs
docker compose up -d --scale worker=2
```

Use `make down` to stop the stack and retain its data. The [deployment guide](docs/DEPLOYMENT.md) covers the migration gate, Docker networking, optional signed webhook demo, CI, and restart tests.

## Local development with host Go

Install Go 1.27.x, Make, and Docker with Compose. Run these commands from the repository root.

```sh
cp .env.example .env
make migrate
make test-schema
make build test vet
export RELAY_DATABASE_URL=postgres://relay:relay_local@localhost:5432/relay
export RELAY_API_KEY="$(openssl rand -hex 32)"
./bin/relay-api &
./bin/relay-worker &
```

`make migrate` starts PostgreSQL, waits for its healthcheck, and applies pending migrations. Running it again preserves existing data. The migration script takes a transaction-scoped advisory lock so concurrent invocations run one at a time. The schema changes and their version record commit together. For future changes, add a numbered SQL file and register its version in `migrations/apply.psql`.

`make test-schema` checks defaults and constraints against the local database and rolls back its test rows. `make db-up` starts the database on its own. `make db-down` stops it and retains the named data volume. Compose binds the database port to localhost. Edit `.env` before the first startup to customize the local development credentials or host port. PostgreSQL reads the initialization credentials when it creates the volume.

## Using the HTTP API

The commands above start both processes in the current shell. The API requires `RELAY_DATABASE_URL` and `RELAY_API_KEY`. Its default listener is `127.0.0.1:8080`; override it with `RELAY_API_ADDR`. Keep credentials in the environment and use TLS at a trusted proxy for remote access. Set `RELAY_MAX_ACTIVE_JOBS` to a positive integer on every API replica to limit outstanding work. Zero (default) disables the limit.

```sh
curl -sS -H "Authorization: Bearer $RELAY_API_KEY" -H 'Content-Type: application/json' \
  -d '{"type":"demo.sleep","payload":{},"idempotency_key":"first-demo"}' \
  http://127.0.0.1:8080/v1/jobs
curl -sS -H "Authorization: Bearer $RELAY_API_KEY" http://127.0.0.1:8080/v1/stats
```

The first response is HTTP 201 with an ID, a queued status, and a scheduled timestamp. Repeat the submission to receive HTTP 200 and the current state of the same job. For example, a stats response after one successful job can look like this:

```json
{"counts":{"queued":0,"running":0,"succeeded":1,"dead":0,"cancelled":0},"queue_depth":0,"running":0,"dead":0,"oldest_queued_age_seconds":null,"completions_last_60s":1}
```

See [the API reference](docs/API.md) for job inspection, cancellation, retry, pagination, limits, and error responses. The full race-enabled test command below also runs HTTP integration tests against two worker instances.

## Architecture

| Component | Intended responsibility |
| --- | --- |
| `cmd/relay-api` | Accept submissions and expose job status over HTTP. |
| `cmd/relay-worker` | Claim eligible jobs and execute handlers with bounded concurrency. |
| `jobs` | Store the payload, current state, next eligible time, retry budget, and current lease. |
| `job_attempts` | Record each claim, its worker and token, timing, outcome, and error. |

A job has one of five states: `queued`, `running`, `succeeded`, `dead`, or `cancelled`. A queued job's `available_at` supports both delayed submission and retry backoff. The optional idempotency key is unique across submissions. A client can reuse a key when it is unsure whether a submission reached the API. Partial indexes cover queued jobs by eligibility time and running jobs by lease expiration.

## State transitions

| Operation | Transition | Rule |
| --- | --- | --- |
| Enqueue | New → queued | A global key identifies one submission. Reuse returns the current job if kind, JSON value, priority, requested schedule, and effective attempt limit match. |
| Claim | Queued → running | Select due jobs by descending priority, then eligibility time and ID. Skip locked rows. Increment the counter and insert one open attempt with a fresh token atomically. |
| Heartbeat | Running → running | The owning worker can extend its live lease using PostgreSQL time. |
| Complete | Running → succeeded | Close the open attempt and record the job completion time. |
| Fail or reap | Running → queued or dead | Close the attempt as failed or expired. Clear ownership. Exhausted jobs become dead. |

Workers execute handlers after the claim commits. Completion, failure, and heartbeat lock the job and check its worker, token, state, and deadline. A worker that resumes after losing its lease cannot overwrite a newer attempt. Reapers skip locked jobs and process at most the requested batch size. A queued job can be cancelled. Retrying a dead job grants one more attempt and retains its history.

The default limit is five attempts. Each retry waits a random duration between half and all of a ceiling that starts at one second, doubles per attempt, and caps at five minutes. Scheduling uses PostgreSQL timestamp precision (microseconds). An omitted schedule means immediately eligible and remains distinct from an explicit timestamp. Migration 002 infers the original schedule for Phase 0 rows from their stored eligibility and creation times.

Delivery is **at least once**. A webhook recipient might accept a request just before a worker crashes. Recovery can cause a second delivery, so handlers should use an idempotency mechanism for external effects.

## Testing the engine

Run the following against the local Compose instance with its default credentials. Adjust the connection string for custom settings.

```sh
make migrate
RELAY_TEST_DATABASE_URL=postgres://relay:relay_local@localhost:5432/relay go test -count=1 -race ./...
```

Each integration test creates its own schema, applies the versioned SQL files, and removes that schema on cleanup. The database user needs schema creation privileges. Without the connection string, Go skips these database tests. Tests force expiration and retry eligibility through database timestamps so they can exercise recovery without sleeping.

## Running workers

Start a second process by running the same binary in another terminal with the same database URL. Migrate the database before startup. The worker reads the process environment. Compose loads its database settings from `.env`.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `RELAY_DATABASE_URL` | Required | PostgreSQL URL for the migrated Relay database |
| `RELAY_WORKER_ID` | Random per process | Identity recorded in attempt history and structured logs |
| `RELAY_CONCURRENCY` | 4 | Maximum simultaneous handlers per process |
| `RELAY_LEASE` | 30s | Lease length. Renewal interval is one third of this value. |
| `RELAY_POLL_INTERVAL` | 250ms | Per-slot wait after an empty claim or database error |
| `RELAY_REAP_INTERVAL` | 5s | Time between scans for expired attempts |
| `RELAY_HANDLER_TIMEOUT` | 1m | Maximum time before the handler context is cancelled |
| `RELAY_SHUTDOWN_GRACE` | 20s | Time given to active jobs after SIGINT or SIGTERM |

Each slot claims one job and calls its handler directly. A slot stays occupied until that handler returns. A running job has one heartbeat goroutine. Before dispatch, the worker confirms ownership. A failed renewal cancels the handler context and leaves the attempt for recovery. A failed final database write is logged and the worker leaves PostgreSQL to determine the outcome. A timeout, panic, handler error, or unknown job type consumes an attempt under the queue's retry policy.

On shutdown, polling and recovery stop. Existing handlers can finish and renew leases during the grace period. When grace expires, the worker cancels remaining work and exits. Cancellation is cooperative. Go cannot interrupt an arbitrary handler or undo an external request. A handler ignoring a timeout can occupy its slot until it returns or the process exits; heartbeats stop on timeout. Make external effects idempotent because a later attempt can overlap or repeat an earlier effect.

Every worker scans for expired leases at startup and periodically thereafter, recovering up to its concurrency limit per scan. A replacement process can recover work left by a crashed process. Multiple reapers use the queue's row locking and can run together.

### Try a job

With a worker running, execute the following against the default local Compose database. The long-running handler accepts a Go duration, observes cancellation, and can be used to see heartbeats. The failing handler below reaches the dead state after two attempts.

```sh
docker compose exec -T db psql -U relay -d relay <<'SQL'
INSERT INTO jobs (kind, payload, max_attempts) VALUES
  ('demo.sleep', '{}', 1),
  ('demo.wait', '{"duration":"12s"}', 2),
  ('demo.fail', '{}', 2);
SQL
docker compose exec db psql -U relay -d relay -c 'SELECT kind,state,attempt_count,lease_expires_at FROM jobs ORDER BY created_at DESC LIMIT 10'
```

For the automated 1,000-job demonstration, set the test URL and run:

```sh
export RELAY_TEST_DATABASE_URL=postgres://relay:relay_local@localhost:5432/relay
go test -race -count=1 -v ./internal/worker -run '^TestTwoWorkers1000$'
```

The demo starts two independent worker instances with separate connection pools and three execution slots each. It checks terminal states and claim history in PostgreSQL and counts handler invocations separately. Run the full test command above to exercise retries, heartbeat loss, graceful draining, and forced abandonment.

## Recurring schedules

Each API process runs one scheduler alongside its HTTP server. Start the API and at least one worker using the local development commands above. In the same shell, create a schedule that submits a short demonstration job every minute. These commands use `jq` to extract its ID.

```sh
base=http://127.0.0.1:8080/v1
schedule_id=$(curl -fsS -H "Authorization: Bearer $RELAY_API_KEY" \
  -H 'Content-Type: application/json' -d '{"name":"Minute demo","cron":"* * * * *","type":"demo.sleep","payload":{}}' \
  "$base/schedules" | jq -r .id)
curl -fsS -H "Authorization: Bearer $RELAY_API_KEY" "$base/schedules/$schedule_id"
curl -fsS -H "Authorization: Bearer $RELAY_API_KEY" "$base/schedules?limit=10"
```

After the next UTC minute and scheduler poll, inspect the resulting jobs. Then disable future occurrences.

```sh
curl -fsS -H "Authorization: Bearer $RELAY_API_KEY" "$base/jobs?type=demo.sleep"
curl -fsS -X DELETE -H "Authorization: Bearer $RELAY_API_KEY" "$base/schedules/$schedule_id"
```

Expressions use five fields in UTC. For example, `*/15 * * * *` runs every quarter hour, and `0 9 * * MON-FRI` runs on weekdays at 09:00 UTC. The first execution is the next matching minute after creation. Configure the poll interval through `RELAY_SCHEDULER_INTERVAL` (Go duration, default `1s`, minimum `1ms`). A poll handles up to 64 due schedules in separate transactions. Job start time also depends on worker availability.

Each transaction locks one due schedule with `FOR UPDATE SKIP LOCKED`. Other replicas can dispatch different schedules. PostgreSQL enforces a unique pair of schedule ID and occurrence time on the jobs table. Job insertion, admission control and schedule advancement commit together. The worker receives an ordinary job with default priority and retry budget. A persisted occurrence can have several execution attempts.

**After an outage**, Relay submits the oldest pending occurrence once and advances to the first match strictly after the database time observed during dispatch. All intervening occurrences are skipped. This bounds catch-up work to one job per overdue schedule. Capacity rejection or a failed transaction leaves the occurrence due for another poll. DELETE disables the schedule; a job that committed before disabling continues normally. Run the full race-enabled database suite above or target `./internal/scheduler ./internal/api` with the same test database URL.

## Observability

The API exposes Prometheus metrics on a separate loopback listener at `http://127.0.0.1:9091/metrics`. Set `RELAY_API_METRICS_ADDR` to select another address or to an empty string to disable it. Worker listeners are opt-in: set `RELAY_WORKER_METRICS_ADDR=127.0.0.1:9092` for one worker and choose a different port for each additional local worker. Counters belong to individual processes. API metrics also include a PostgreSQL snapshot of queue depth and eligible-job age. See the [observability guide](docs/OBSERVABILITY.md) for scrape configuration, PromQL, and troubleshooting.

## Webhook demonstration

Workers accept `webhook.deliver` jobs for operator-configured destinations. Relay signs the JSON event, sends it over HTTP, and records the result using ordinary job attempts. The local receiver records one durable inbox entry per event ID. The [webhook guide](docs/WEBHOOKS.md) includes setup, signing, retry rules, and a crash-and-redelivery demonstration.
