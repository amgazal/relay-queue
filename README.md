# Relay

Relay is a Go background job queue backed by PostgreSQL 17. Phase 0 provides the database schema, a local development environment, and two executable entry points. Both commands currently print a startup message and exit.

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

## Execution model planned for later phases

A worker will use a short transaction and `FOR UPDATE SKIP LOCKED` to claim work. This lets another worker skip rows already locked by its peers. Each claim will create an attempt and a fresh token. The worker will commit before running the handler. Heartbeats and completion updates must match the active token and an unexpired lease. Recovery will close expired attempts and either return the job to the queue or mark it dead when its attempts are exhausted.

Delivery will be **at least once**. For example, a webhook recipient might accept a request just before the worker crashes. The next worker may send it again. Submission keys prevent duplicate job records, while handlers and recipients must account for repeated execution.

Queue operations, the pgx data access layer, HTTP endpoints, worker execution, and webhook delivery are future phases. This foundation includes executable smoke tests and live schema checks. Testcontainers integration tests, CI, and benchmarks will accompany later implementation work.
