# Relay operator runbook

## Bring up the local stack

Follow the [quick start](../README.md#quick-start) to set a private API key and start Compose. PostgreSQL must become healthy and the one-shot migration must exit successfully before the application services start. Use the same Compose project name and credentials across restarts.

```sh
docker compose ps -a
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
docker compose logs --tail=40 migrate api worker
docker compose up -d --scale worker=2
```

Health checks process liveness; readiness checks a database ping with a one-second timeout. Check migration status separately. Each worker generates its own ID. To stop and retain the database, run `docker compose --profile demo down`. The default 35-second worker container stop deadline leaves time for its 20-second drain period.

Set POSTGRES_USER, POSTGRES_DB, POSTGRES_PASSWORD and POSTGRES_PORT before initializing a volume. The API requires RELAY_API_KEY. Tune RELAY_CONCURRENCY, RELAY_LEASE and RELAY_HANDLER_TIMEOUT together: a long handler needs capacity and heartbeats throughout execution. RELAY_MAX_ACTIVE_JOBS counts queued and running jobs. Match this setting across API replicas. See the [full environment table](DEPLOYMENT.md#configuration) and [metrics settings](OBSERVABILITY.md).

## Walk through retry and scheduling

From the quick-start shell, use the following helper to call the local authenticated API. These commands create demonstration records. Use a development database.

```sh
relay() { curl -fsS -H "$auth" -H 'Content-Type: application/json' "$@"; }
id() { python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'; }

failed=$(relay -d '{"type":"demo.fail","payload":{},"max_attempts":2}' "$base/jobs" | id)
relay "$base/jobs/$failed"
relay "$base/jobs?status=dead"
```

Poll the job URL until it reaches dead. Inspect its attempt_count, max_attempts and last_error. After addressing a real failure's cause, use the retry endpoint. For demo.fail another failure is intentional.

```sh
relay -X POST "$base/jobs/$failed/retry"
schedule=$(relay -d '{"name":"Minute demo","cron":"* * * * *","type":"demo.sleep","payload":{}}' "$base/schedules" | id)
relay "$base/schedules/$schedule"
```

After the next UTC minute and poll interval, inspect the jobs, then disable further dispatch. Existing jobs continue.

```sh
relay "$base/jobs?type=demo.sleep"
relay -X DELETE "$base/schedules/$schedule"
```

For a signed webhook, follow the [Compose demo](DEPLOYMENT.md#optional-webhook-profile) and submit its fixed event ID twice. Inspect the unique inbox row. For repeatable assertions use `make smoke` and `make bench-chaos` in separate, automatically named projects. Neither command removes the project's development database.

## Diagnose stalled work

1. Check `/readyz`, migration exit status and worker logs. Look for database connectivity errors, a full admission budget (HTTP 429), or a handler name missing from the registry.
2. Inspect `GET /v1/stats`, then list queued and running jobs. Check scheduled_at before interpreting queued jobs as stalled. Admission and backlog include delayed jobs.
3. Compare oldest eligible age, active handlers, completion rate, retry reasons and expired-lease recoveries. An unavailable database snapshot omits depth and age; the API exports relay_queue_snapshot_up=0. Use one database depth series per queue.
4. For a particular job, inspect attempt_number, worker_id, started_at, finished_at and outcome in job_attempts. Treat stored errors as private. Never share a claim token or payload when reporting a failure.
5. After a worker crash, allow lease expiration, the next reaper scan and retry backoff. A dead job needs a manual retry; a running job cannot be cancelled through the API.

To inspect the container-local API metrics listener:

```sh
docker run --rm --network "container:$(docker compose ps -q api)" curlimages/curl:8.12.1 \
  -fsS http://127.0.0.1:9091/metrics
```

## Protect and restore state

A named Docker volume preserves data across ordinary restarts. Back up the database separately and protect backups as sensitive data: they contain payloads, attempt history and receiver event IDs. Example for the local default database, from a shell with a private backup directory outside the repository:

```sh
umask 077
backup="$HOME/relay-backups/relay-$(date +%Y%m%d-%H%M%S).dump"
mkdir -p "$HOME/relay-backups"
docker compose exec -T db pg_dump -U relay -d relay -Fc > "$backup" &&
  docker compose exec -T db pg_restore --list < "$backup" >/dev/null
```

A readable archive is only the first check. Restore into a fresh database in an isolated environment, with network egress disabled and workers and schedulers stopped. Use pg_restore with --exit-on-error, validate the schema version and row counts, then test a disposable job before a planned cutover. Restore the matching configuration and signing secrets through a separate secure channel. A database snapshot cannot roll back effects committed by an external receiver. Reconcile those effects before resuming delivery.

The local deployment has one database, one shared API credential, and operator-managed storage and TLS. It includes neither automated failover nor a tested recovery-time objective. A dashboard could later provide a read-only view of queue depth, eligible age and attempt histories; the current CLI walkthrough and failure evidence already demonstrate the backend behavior.
