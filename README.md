# Relay

Relay is a Go background job queue backed by PostgreSQL 17. Phase 1 implements the durable queue engine in `internal/queue` using pgx/v5 and a shared connection pool. Both commands currently print a startup message and exit.

## Local development

Install Go 1.27.x, Make, and Docker with Compose. Run these commands from the repository root.

```sh
cp .env.example .env
make migrate
make test-schema
make build test vet
./bin/relay-api
./bin/relay-worker
```

`make migrate` starts PostgreSQL, waits for its healthcheck, and applies pending migrations. Running it again preserves existing data. The migration script takes a transaction-scoped advisory lock so concurrent invocations run one at a time. The schema changes and their version record commit together. For future changes, add a numbered SQL file and register its version in `migrations/apply.psql`.

`make test-schema` checks defaults and constraints against the local database and rolls back its test rows. `make db-up` starts the database on its own. `make db-down` stops it and retains the named data volume. Compose binds the database port to localhost. Edit `.env` before the first startup to customize the local development credentials or host port. PostgreSQL reads the initialization credentials when it creates the volume.

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

Workers execute handlers after the claim commits. Completion, failure, and heartbeat lock the job and check its worker, token, state, and deadline. A worker that resumes after losing its lease cannot overwrite a newer attempt. Reapers skip locked jobs and process at most the requested batch size. Cancelled is reserved for a future management operation.

The default limit is five attempts. Each retry waits a random duration between half and all of a ceiling that starts at one second, doubles per attempt, and caps at five minutes. Scheduling uses PostgreSQL timestamp precision (microseconds). An omitted schedule means immediately eligible and remains distinct from an explicit timestamp. Migration 002 infers the original schedule for Phase 0 rows from their stored eligibility and creation times.

Delivery is **at least once**. A webhook recipient might accept a request just before a worker crashes. Recovery can cause a second delivery, so handlers should use an idempotency mechanism for external effects.

## Testing the engine

Run the following against the local Compose instance with its default credentials. Adjust the connection string for custom settings.

```sh
make migrate
RELAY_TEST_DATABASE_URL=postgres://relay:relay_local@localhost:5432/relay go test -count=1 -race ./...
```

Each integration test creates its own schema, applies the versioned SQL files, and removes that schema on cleanup. The database user needs schema creation privileges. Without the connection string, Go skips these database tests. Tests force expiration and retry eligibility through database timestamps so they can exercise recovery without sleeping.

The API, worker pool, webhook delivery, and background reaper loop are future phases. Applications using the package must call recovery periodically for abandoned jobs to progress.
