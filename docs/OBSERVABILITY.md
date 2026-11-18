# Monitoring Relay

Each API and worker process owns a Prometheus registry. Scrape every process whose activity you want to measure. The scheduler shares its API process's registry. Database transitions update counters after successful commit. An idempotent replay contributes an HTTP request but no new job. The metrics listener serves GET /metrics separately from the authenticated REST API.

## Start and scrape

For host-built executables, run `make migrate build`, export RELAY_DATABASE_URL and RELAY_API_KEY, and start each process in a separate terminal sharing that environment. For containers, use the [deployment metrics instructions](DEPLOYMENT.md#monitoring).

```sh
RELAY_API_METRICS_ADDR=127.0.0.1:9091 ./bin/relay-api
RELAY_WORKER_METRICS_ADDR=127.0.0.1:9092 ./bin/relay-worker
```

```sh
curl -fsS http://127.0.0.1:9091/metrics
curl -fsS http://127.0.0.1:9092/metrics
```

The API defaults to 127.0.0.1:9091. An explicitly empty address disables it. Worker metrics default to disabled. Give additional processes their own ports; for ad hoc tests, use 127.0.0.1:0 and read the assigned address from the startup log. A configured listener that cannot bind causes startup to fail. Listeners have HTTP timeouts, allow at most two simultaneous scrapes and drain on shutdown.

Example for Prometheus running on the same host:

```yaml
scrape_configs:
  - job_name: relay-api
    scrape_interval: 15s
    static_configs:
      - targets: ['127.0.0.1:9091']
        labels: {queue: local}
  - job_name: relay-worker
    scrape_interval: 15s
    static_configs:
      - targets: ['127.0.0.1:9092']
        labels: {queue: local}
```

Include each additional listener in its role's targets. Use a distinct queue label per independent database. A container's loopback refers to that container. Metrics endpoints have no authentication or TLS. Use loopback or a protected monitoring network; keep the API key on the REST interface. Scrapes contain aggregate data only. The existing authenticated job detail endpoint can return handler errors; handlers must still keep credentials out of error messages.

## Reading metrics

A **counter** accumulates events until its process restarts. Apply rate() per target before summing across targets so restarts are handled correctly. A **gauge** is a current measurement that can rise or fall. A **histogram** counts observations in duration buckets; combining bucket rates lets Prometheus estimate percentiles across instances.

| Series (prefix relay_) | Measurement |
| --- | --- |
| jobs_submitted_total, submission_conflicts_total | Newly committed jobs, including scheduled jobs, and conflicting idempotency-key submissions |
| claim_duration_seconds | Wall time of each claim call, including empty polls and database waits |
| execution_attempts_total, jobs_completed_total, failed_attempts_total | Dispatches into the handler registry and committed success/failure transitions. A crash or lost lease can leave a started dispatch without a locally recorded finalization. |
| handler_duration_seconds, active_handlers | Time spent in handler dispatch and number of dispatches still executing |
| retries_total | Committed retries with reason failure, expired, or manual |
| lease_renewal_failures_total, expired_attempts_recovered_total, worker_timeouts_total | Failed initial confirmations or heartbeats, expired attempts finalized by a reaper, and handler deadline observations |
| http_requests_total, http_request_duration_seconds | Response counts by registered route and status; latency by registered route. HTTP 401 counts authentication failures. The separate scrape endpoint is excluded. |
| scheduler_occurrences_dispatched_total, scheduler_occurrences_skipped_total | New occurrence jobs and intermediate UTC cron matches skipped by committed schedule advances |
| scheduler_dispatch_failures_total, scheduler_operation_duration_seconds | Unsuccessful dispatch operations (including admission rejection) and operation time (including idle checks) |
| queue_depth, oldest_eligible_age_seconds, queue_snapshot_up | Database snapshot by state, seconds since the oldest eligible queued job became ready, and snapshot success (1 or 0) |

Each API scrape performs one aggregate query with a one-second deadline. All five depths use the same snapshot. Queued depth includes delayed jobs; eligible age excludes future jobs and is zero when nothing is eligible. A failed query omits depths and age and exports snapshot_up=0; process metrics remain available. The REST stats endpoint's oldest queued age uses creation time and includes delayed jobs. Replicas expose views of the same rows. Use one target or group by database with max; summing depths across replicas double-counts jobs.

## Example PromQL

```promql
# Successful completions per second
sum by (queue) (rate(relay_jobs_completed_total[5m]))

# Failed attempts per second
sum by (queue) (rate(relay_failed_attempts_total[5m]))

# Approximate p95 handler duration in seconds
histogram_quantile(0.95, sum by (queue, le) (rate(relay_handler_duration_seconds_bucket[5m])))

# Latest sampled queued depth across API replicas
max by (queue) (relay_queue_depth{state="queued"})

# Expired attempts recovered per second
sum by (queue) (rate(relay_expired_attempts_recovered_total[5m]))
```

## Investigating problems

- If eligible age rises, check Prometheus up for each worker, active_handlers against configured concurrency, and completion rate. A deep queue with zero eligible age can consist entirely of delayed jobs.
- If failures increase, group retry rate by reason. Compare handler durations and timeouts for slow execution. Rising lease renewal failures followed by recoveries point to lost ownership or database connectivity issues. Recovery can happen on a different worker.
- If scheduling falls behind, compare dispatch failures, active queue depth and skipped occurrences. Admission limits can defer dispatch until capacity becomes available.
- JSON lifecycle logs include a component and relevant worker, job, attempt or request identifiers. Server-side request failures are warnings; normal request timing is debug-level. Operational logs use error categories. Match a request ID to its response header without placing it in a metric label.

Counters are best-effort observations, not an audit trail: a crash between commit and observation, a restart between scrapes, or an unmonitored producer can leave gaps. PostgreSQL job and attempt rows remain the durable record. Quantiles are bucket estimates; values above the largest finite bucket have limited resolution. Exact database snapshots scan the jobs table and should be scraped at a cadence appropriate to database size. Data from different replicas can differ briefly because their scrapes occur at different times.
