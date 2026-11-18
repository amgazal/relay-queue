# Relay

Relay is a durable background job queue written in Go. An HTTP client submits a job, PostgreSQL stores it, and independent workers claim it when they have capacity. Relay supports delayed jobs, recurring UTC schedules, and signed webhook delivery.

**What happens if a worker dies after sending a webhook?** Relay retries the job. The receiver can use the stable event ID to apply its effect once. In a four-worker crash test, all 10,003 accepted job IDs were accounted for: 10,002 succeeded and one intentionally exhausted its attempt budget. Two deliveries of the interrupted webhook produced one durable inbox entry. [Crash protocol and evidence](docs/BENCHMARKS.md)

```mermaid
flowchart LR
    Client -->|authenticated HTTP| API[API and scheduler]
    API -->|submit or schedule| DB[(PostgreSQL 17)]
    Workers[Go workers] <-->|claim, renew, finalize| DB
    Workers -->|signed HTTPS| Receiver[Demo receiver]
    Receiver -->|unique event ID| DB
```

Jobs run **at least once**. Short transactions using `FOR UPDATE SKIP LOCKED` let workers claim different rows concurrently. A fresh token and expiring lease protect each claim. A separate attempt table preserves the history of retries and crash recovery. Every job type uses this same execution path.

## Quick start

From a fresh clone, use Docker with Compose, OpenSSL, curl and Python 3. Run these commands in the same shell. PostgreSQL and the API bind to host loopback by default.

```sh
git clone https://github.com/amgazal/relay-queue.git
cd relay-queue
cp -n .env.example .env
export RELAY_API_KEY="$(openssl rand -hex 32)"
docker compose up --build -d --wait
curl -fsS http://127.0.0.1:8080/readyz

base=http://127.0.0.1:8080/v1
auth="Authorization: Bearer $RELAY_API_KEY"
job_id=$(curl -fsS -H "$auth" -H 'Content-Type: application/json' \
  -d '{"type":"demo.sleep","payload":{},"idempotency_key":"first-job"}' "$base/jobs" |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -fsS -H "$auth" "$base/jobs/$job_id"
docker compose up -d --scale worker=2
```

Poll the job URL again to see its terminal state. An identical submission with the same key returns the original job. Use `docker compose --profile demo down` to stop the stack and retain its volumes.

## Demonstrate the failure cases

| Command | What it checks |
| --- | --- |
| `make smoke` | Starts an isolated stack, retries a failing job, dispatches a recurring schedule, sends signed webhooks, scales workers and verifies restart persistence. |
| `make bench-chaos` | Kills workers after a receiver commit, checks stale-attempt recovery, and reconciles all accepted IDs against PostgreSQL. |
| `make bench-verify` | Verifies the 22 saved benchmark trials without launching services. |

For an interactive walkthrough, the [runbook](docs/RUNBOOK.md) covers job inspection, retries and recurring schedules. The [Compose webhook demo](docs/DEPLOYMENT.md#optional-webhook-profile) uses a configured HTTPS destination. Submitting its example twice creates two jobs and one receiver effect.

## Measured performance

Local Apple M1, eight Docker CPUs, PostgreSQL 17.12. Each figure below is the median of three 2,000-job bursts; each worker had four execution slots.

| Workload | Workers | Completed jobs/s | Enqueue p99 | End-to-end p99 |
| --- | ---: | ---: | ---: | ---: |
| API burst with no-op handler | 4 | 1,466 | 24.52 ms | 398 ms |
| Signed HTTPS webhook with durable receiver write | 4 | 935 | 28.90 ms | 804 ms |
| No-op worker-scaling burst | 8 | 1,346 | 27.66 ms | 60.85 ms |

Admission rate counts new jobs; completion throughput includes drain time. The eight-worker result belongs to a separate scaling series. Hardware, run order, sustained-load results, and variability are documented with the [methodology](docs/BENCHMARKS.md) and [saved measurements](benchmarks/results.json).

## Development and CI

With Go 1.27.x, Make and the default local database credentials:

```sh
make test-schema
export RELAY_TEST_DATABASE_URL=postgres://relay:relay_local@127.0.0.1:5432/relay
make test-integration
```

CI runs formatting, vet, build, the race-enabled Go suite against PostgreSQL, schema checks and the Compose smoke test. Full benchmarks run separately. [CI workflow](.github/workflows/ci.yml)

## Design and operations

| Guide | Use it for |
| --- | --- |
| [Architecture](docs/ARCHITECTURE.md) · [Design decisions](docs/DESIGN.md) | Claim and lease diagrams, state transitions, scheduling policy and tradeoffs |
| [HTTP API](docs/API.md) | Authentication, submission, inspection, pagination and schedules |
| [Deployment](docs/DEPLOYMENT.md) · [Runbook](docs/RUNBOOK.md) | Configuration, startup, backups and diagnosing stalled work |
| [Webhooks](docs/WEBHOOKS.md) · [Observability](docs/OBSERVABILITY.md) | Signatures, durable deduplication, metrics and PromQL |

All API key holders can manage all jobs. Metrics endpoints are unauthenticated and intended for protected networks. PostgreSQL availability and durable storage are operational dependencies. The local benchmark measures short runs on a shared machine. External effects require handler-specific idempotency.

## License

License selection is pending.
