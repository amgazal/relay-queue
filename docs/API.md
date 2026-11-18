# Relay HTTP API

The local base URL is `http://127.0.0.1:8080`. Supply `Authorization: Bearer <key>` on all `/v1` requests. Configure a high-entropy key through `RELAY_API_KEY`. All holders have full access to all jobs. Use TLS when crossing a network boundary. Responses include a server-generated `X-Request-ID`. Logs include the route pattern, method, status, duration and request ID.

## Submit a job

`POST /v1/jobs` accepts one JSON object of at most 1 MiB. Send `Content-Type: application/json`. Unknown fields, duplicate fields, trailing JSON, and non-integral numeric parameters are invalid.

| Field | Rules |
| --- | --- |
| type | Required nonblank string, at most 128 UTF-8 bytes. Workers resolve handler names at execution time. |
| payload | Required JSON value, including null, representable as PostgreSQL jsonb. |
| priority | Integer from −1000 to 1000. Default 0. Higher runs first among eligible jobs. |
| max_attempts | Integer from 1 to 1000. Default 5. |
| delay_seconds | Integer from 0 to 31536000. Default 0. Relative to submission in PostgreSQL. |
| idempotency_key | Optional nonblank string up to 256 UTF-8 bytes. Globally unique. |

Example request:

```json
{"type":"demo.wait","payload":{"duration":"2s"},"delay_seconds":10,"idempotency_key":"example-1"}
```

Example creation response (201):

```json
{"id":"14fb7078-456a-49a7-a143-a687464bcb8f","type":"demo.wait","status":"queued","priority":0,"attempt_count":0,"max_attempts":5,"scheduled_at":"2026-11-17T15:00:10Z"}
```

The Location header points to the job. Identical replays receive HTTP 200 and current metadata. Changing a submission parameter with the same key returns 409. The original relative delay and original attempt budget are used for comparison across retries.

## Inspect and list

`GET /v1/jobs/{id}` takes a UUID. Returns 200 with the same public fields as submission, plus `created_at`, and optional `completed_at` (successful completion) and `last_error` (most recent recorded handler error). The scheduled time is current eligibility and can move after failure. The API omits payload, worker identity, and ownership token. Handler authors should keep credentials and private payload data out of error messages.

`GET /v1/jobs?status=queued&type=demo.wait&limit=25` returns a page. All parameters are optional. Valid statuses are queued, running, succeeded, dead, and cancelled. The type filter is exact. Limit is 1–100, default 25. An empty page is:

```json
{"jobs":[],"next_cursor":null}
```

For subsequent pages, pass the returned opaque string as `cursor` with the same filters. Ordering is newest creation first, then descending UUID. Pages reflect live data; jobs can leave a filtered result between requests.

## Change job state

| Endpoint | Behavior |
| --- | --- |
| `POST /v1/jobs/{id}/cancel` | Atomically changes queued to cancelled. A worker that already claimed the job retains ownership. |
| `POST /v1/jobs/{id}/retry` | Atomically returns a dead job to the queue, makes it immediately eligible, and increases its maximum attempts by one. Attempt numbers continue from the last claim. |

Send an empty body. Both return the updated job detail on success (200), 404 for an unknown job, or 409 for a disallowed transition. A budget at PostgreSQL's integer maximum cannot be extended.

## Stats and health

`GET /v1/stats` returns counts for all five states, queue_depth (all queued jobs including delayed jobs), running and dead counts, oldest_queued_age_seconds (since creation, null if empty), and completions_last_60s (successful completions during the trailing minute). All metrics come from a single database snapshot.

`GET /healthz` returns 200 with `{"status":"alive"}`. `GET /readyz` returns 200 with `{"status":"ready"}` after a successful database ping or 503 if the ping fails or exceeds one second. Both probes are public.

## Admission and errors

`RELAY_MAX_ACTIVE_JOBS` limits the sum of queued and running jobs. A claim or automatic retry keeps its slot. Cancellation, permanent failure and success release it. When enabled, admissions acquire the same PostgreSQL transaction advisory lock. A new submission or manual retry that would exceed capacity rolls back. Identical replays remain available at capacity. Configure all API replicas identically and submit through the API for this limit to hold. Direct SQL and unrestricted in-process producers bypass it. Serialized admissions and exact counts trade write throughput for a strict limit.

Errors use the following envelope:

```json
{"error":{"code":"QUEUE_FULL","message":"Admission limit reached."}}
```

| HTTP | Meaning |
| --- | --- |
| 400 | Invalid JSON, input, UUID, or pagination parameter |
| 401 | Missing or incorrect bearer credential |
| 404 | Job or endpoint absent |
| 409 | Idempotency conflict or invalid transition |
| 413 | Submission body exceeds 1 MiB |
| 429 | Active-job admission limit reached |
| 500 | Unexpected internal failure |
| 503 | Database unavailable or operation timed out |

An interrupted response can leave a client uncertain whether a submission committed. Retry with the same idempotency key. Workers provide at-least-once execution; external effects still require idempotent handlers.

## Recurring schedules

All four schedule endpoints require the same bearer credential as jobs. Schedule timestamps are PostgreSQL instants. Cron matching uses UTC.

| Method and path | Result |
| --- | --- |
| POST /v1/schedules | 201 with schedule and Location header. Each successful POST creates a distinct schedule, including repeated names. |
| GET /v1/schedules | 200 with `{"schedules":[...],"next_cursor":null}`. Includes disabled schedules. Accepts the same limit (default 25, maximum 100) and cursor format as jobs, ordered by descending creation time and ID. |
| GET /v1/schedules/{id} | 200 with the schedule, or 404 for a missing UUID. |
| DELETE /v1/schedules/{id} | 200 with the disabled schedule, including on repeated deletion. Already enqueued jobs retain their state. If dispatch wins the row lock first, its job remains queued or executing. |

Example creation request:

```json
{"name":"Weekday report","cron":"0 9 * * MON-FRI","type":"demo.sleep","payload":{}}
```

Example response:

```json
{"name":"Weekday report","cron":"0 9 * * MON-FRI","type":"demo.sleep","payload":{},"id":"14fb7078-456a-49a7-a143-a687464bcb8f","enabled":true,"next_run":"2026-11-18T09:00:00Z","created_at":"2026-11-17T15:00:00Z","updated_at":"2026-11-17T15:00:00Z"}
```

All four request fields are required. Name and type are nonblank strings of up to 128 UTF-8 bytes. Payload can be any PostgreSQL jsonb value, including null. The existing 1 MiB body limit, strict field validation, JSON errors, and UUID validation apply. Schedules store and return the payload to authenticated clients. A disabled schedule retains its last planned next_run for inspection.

Cron expressions (up to 256 bytes) use minute, hour, day of month, month, and day of week in order. The parser supports wildcards, lists, ranges, steps, and standard three-letter month and weekday names. Sunday is 0. When both day fields are restricted, either may match. Seconds, descriptors such as `@daily`, question marks and time-zone overrides are rejected. Creation requires a calculable future match.

Every accepted occurrence uses one job slot under the active-job limit. A full queue defers dispatch. Overdue schedules submit their oldest outstanding occurrence once and skip intermediate matches up to dispatch time. Generated jobs use priority zero and five maximum attempts. Future dispatch requires a running API process; execution requires a worker registered for the job type.
