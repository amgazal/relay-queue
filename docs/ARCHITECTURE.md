# Architecture

PostgreSQL owns the state of every job. The API accepts submissions, supports inspection and guarded management transitions, and runs one scheduler loop per process. A cron occurrence enters the same jobs table as an immediate submission. Independent workers claim jobs only when a local execution slot is free. See the [system diagram](../README.md) and [deployment topology](DEPLOYMENT.md).

```mermaid
flowchart LR
    Client -->|bearer-authenticated jobs and schedules| API[API process]
    API -->|submissions and management| DB[(PostgreSQL)]
    API --> S[Scheduler loop]
    S -->|dispatch due occurrence| DB
    DB <-->|claim and finalize| W[Worker process]
    W --> H[Registered handler]
    H -->|webhook.deliver| R[Demo receiver]
    R -->|deduplicate and record effect| DB
    Prom[Prometheus] -->|scrape| API
    Prom -->|optional scrape| W
```

## Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> queued: submit or dispatch occurrence
    queued --> running: claim and open attempt
    queued --> cancelled: cancel
    running --> running: heartbeat
    running --> succeeded: complete
    running --> queued: retryable failure or expiration with budget
    running --> dead: permanent failure or exhausted budget
    dead --> queued: manual retry
```

Delayed jobs stay queued until available_at. The job row shows current state; each claim inserts a numbered attempt with its own worker and token. Completion, failure and recovery finalize the open attempt in the same transaction as the state change. Manual retry increases the maximum budget by one and preserves attempt numbers.

## Claim and renew

```mermaid
sequenceDiagram
    participant W as Free worker slot
    participant DB as PostgreSQL
    participant H as Handler
    W->>DB: Select eligible row FOR UPDATE SKIP LOCKED
    DB-->>W: Commit claim, increment attempt, store token and deadline
    W->>DB: Confirm ownership by heartbeat
    W->>H: Invoke with timeout and cancellation context
    loop While handler is active
        W->>DB: Renew with job ID, worker ID and claim token
    end
    H-->>W: Return result
    W->>DB: Lock job, check live lease, finalize attempt and job
```

The handler runs outside a database transaction. A lost or uncertain renewal cancels its context. A handler that ignores cancellation may continue performing external work. The database still fences stale finalization.

## Recovery after a crash

```mermaid
sequenceDiagram
    participant A as Original worker
    participant DB as PostgreSQL
    participant B as Surviving worker
    A->>DB: Claim job and open attempt 1
    Note over A: Process stops before finalization
    Note over DB: Lease expires on database clock
    B->>DB: Lock expired row, close attempt as expired
    alt Budget remains
        B->>DB: Requeue with backoff
        B->>DB: Claim when eligible with fresh token
    else Budget exhausted
        B->>DB: Mark job dead
    end
```

Each worker also runs a periodic reaper. Multiple reapers can skip one another's locks. Graceful shutdown stops claiming, lets handlers drain with heartbeats, and cancels remaining work after the configured grace period.

## Webhook redelivery

```mermaid
sequenceDiagram
    participant W as Worker webhook handler
    participant R as Demo receiver
    participant DB as PostgreSQL
    W->>R: POST signed bytes with event ID
    R->>DB: Insert inbox row with unique event ID
    Note over W: Worker loses ownership before queue completion
    W->>R: Replacement attempt sends same event ID
    R->>DB: Insert encounters existing event ID
    R-->>W: HTTP 204
    W->>DB: Complete under current lease
```

The sender resolves an operator-configured destination. The receiver verifies the timestamp and signature before inserting its business effect. The queue and receiver use distinct tables; the HTTP acknowledgment and job completion are separate commits.

## Monitoring

The API and each worker own separate Prometheus registries. The API also reads current job counts from PostgreSQL during a scrape. Aggregate process counter rates across instances. Use one database-wide depth measurement per queue. Scraping frequency affects database load. [Metrics and queries](OBSERVABILITY.md)
