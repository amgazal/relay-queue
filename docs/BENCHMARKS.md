# Relay reliability and performance measurements

## Executive summary

In the local 10,000-job failure workload, Relay accounted for all **10,003 accepted jobs**, including three control jobs. **10,002 succeeded and one intentionally reached dead**. After SIGKILL, PostgreSQL recorded 13 expired attempts. The signed webhook was accepted twice and produced one durable receiver effect.

In three 2,000-job no-op bursts per worker count, median completion throughput rose from **947 jobs/s with one process to 1,346 jobs/s with eight**. Median run p95 end-to-end latency, which includes queue wait, fell from **1,271 ms to 40 ms**. The eight-worker runs were close to the producer's admission rate. These are short, local, warm-system results. The [machine-readable report](../benchmarks/results.json) includes 22 verified runs, sampled metrics and execution plans.

## Environment and configuration

Measured November 17, 2026 on an Apple M1, 8 host CPUs, 16 GiB RAM, macOS arm64. Docker Engine 29.7.2 reported eight CPUs and 7.75 GiB available to its Linux VM. API, workers, PostgreSQL, k6 and the HTTPS receiver shared the VM, with no per-service CPU quotas. Go 1.27.4 linux/arm64, PostgreSQL 17.12, k6 1.3.0; repository base e0b6c9a plus the benchmark harness and no-op handler.

Each worker had four slots, an eight-second lease, a 20 ms idle poll, a 500 ms reaper interval, and a 90-second handler timeout. One API ran with admission limiting disabled. PostgreSQL had fsync and synchronous commit enabled, autovacuum on, 100 maximum connections, 128 MiB shared buffers, 4 MiB work memory and a 1 GiB maximum WAL size. Warm-up took 0.64–0.80 seconds per trial. The exact invocation and individual measurement durations are in the report. Its pg_settings strings concatenate PostgreSQL's setting and unit: for example, shared_buffers is 16,384 blocks of 8 KiB.

## Results

The following values are medians across three trials per row. Percentiles are calculated within each trial before taking the median; they are not pooled observations. HTTP rates include one idempotent replay per ten newly admitted jobs. Each performance trial in these tables admitted 2,000 and handled 2,200 requests, with zero unexpected responses or 429 rejections.

| Workload, 4 workers | HTTP req/s | New jobs/s | Completed/s | Enqueue p50 / p95 / p99 (ms) | End-to-end p50 / p95 / p99 (ms) | Drain (s) |
| --- | ---: | ---: | ---: | --- | --- | ---: |
| API burst, no-op | 1,920 | 1,745 | 1,466 | 6.88 / 15.76 / 24.52 | 245 / 392 / 398 | 0.223 |
| 100 new jobs/s for 20 s, 50 ms handler | 110 | 100 | 99.78 | 0.79 / 2.19 / 8.00 | 55.15 / 61.63 / 73.11 | 0.055 |
| Signed HTTPS webhook burst | 1,536 | 1,396 | 935 | 8.82 / 18.80 / 28.90 | 552 / 767 / 804 | 0.708 |

All three webhook trials produced 2,000 distinct receiver effects apiece. In the sustained trials the peak sampled backlog was one queued job and final backlog was zero. Interior samples averaged 4.6–4.8 active handlers out of 16, with a measured mean handler time of about 50.6–50.7 ms. No measured jobs retried in these three workload groups. API burst rates and that 20-second sustained rate answer different capacity questions.

| Processes × 4 slots | Completed/s, median (range) | New jobs/s | Enqueue p95 (ms) | End-to-end p50 / p95 / p99 (ms) | Drain (s) |
| --- | ---: | ---: | ---: | --- | ---: |
| 1 | 947 (941–962) | 2,553 | 10.02 | 869 / 1,271 / 1,311 | 1.321 |
| 2 | 1,199 (1,042–1,264) | 2,421 | 10.33 | 511 / 783 / 801 | 0.803 |
| 4 | 1,288 (1,124–1,347) | 1,589 | 18.72 | 252 / 464 / 483 | 0.330 |
| 8 | 1,346 (1,248–1,393) | 1,355 | 19.61 | 19.81 / 40.09 / 60.85 | 0.018 |

Four to eight workers improved the median completion rate by about 4.5%; their trial ranges overlap. More slots helped consumers keep up, while admission slowed as the API shared database and CPU resources with more workers. At eight workers there was little backlog left to drain. This experiment does not isolate a maximum worker-only or API-only rate. Handler invocation count measures starts; durable successes measure commits. The short trials are especially sensitive to CPU scheduling and cache state.

## Failure accounting

| Large chaos trial | Observed |
| --- | ---: |
| Load jobs + graceful, webhook and deliberate-dead controls | 10,000 + 3 |
| Accepted IDs matched in PostgreSQL / unaccounted IDs | 10,003 / 0 |
| Succeeded / dead / still nonterminal | 10,002 / 1 / 0 |
| Total claims / additional attempts / expired attempts | 10,016 / 13 / 13 |
| Overlapping recorded attempts / invalid histories | 0 / 0 |
| Accepted deliveries of the gated event / durable effects | 2 / 1 |

The measured controller interval was 15.065 seconds, including submission and recovery but excluding the graceful precheck and warm-up. Only the gated webhook has a direct external duplication count. The other 12 additional claims cannot establish how far those handlers executed before termination. Earlier setup trials were excluded, including a SQL verifier error and an interrupted load-script read. Only the complete verified reference suite is summarized here.

## PostgreSQL observations

With 500 temporary queued rows added to the reporting database, the locking candidate selection used jobs_priority_idx and took 0.146 ms with 97 shared buffer hits. The grouped depth query scanned 53,911 job rows and took 10.539 ms with 1,517 shared hits. Example inserts took 0.157 ms for a job and 0.229 ms for an attempt including its foreign-key check. These are warm, single-session EXPLAIN ANALYZE samples in a rolled-back transaction; they exclude the commit cost and do not measure the complete multi-write claim statement. Real claims update the job, create a new token, and insert history atomically; finalizations also write both tables.

Jobs grew from 8,192 to 12,468,224 table bytes and from 65,536 to 17,268,736 index bytes. Attempt history grew from 8,192 to 7,331,840 table bytes and from 24,576 to 7,282,688 index bytes. One- and two-worker scaling trials sampled zero lock waiters; four- and eight-worker peaks were two and four. Database CPU snapshots for those scaling runs ranged from 141% to 225% (100% is one core). Exact depth aggregation costs more as history grows. These measurements did not establish a specific query as the cause of the scaling plateau, so this phase makes no production query or index change and claims no optimization speedup.

For a next experiment, prefill a fixed backlog to isolate worker capacity, place k6 on another host, randomize run order over more repetitions, and collect CPU, disk and WAL time series over minutes. Measure dashboard scrape costs with longer retention before introducing a counter table that would add write contention.

## Reproduce

Run from the repository root with Docker Compose (including BuildKit), Git and Python 3. k6 runs in a container on the private Compose network. Its only HTTP load target is the benchmark API service. The host controller talks to ephemeral loopback ports for probes and metrics. No cloud or development database credentials are needed.

```sh
make bench-chaos           # 200 load jobs plus three control jobs
make bench-chaos-full      # 10,000 load jobs plus three control jobs
make bench-api             # concurrent submissions and idempotent replays
make bench-scale           # 1, 2, 4 and 8 worker processes
make bench-e2e             # sustained arrival rate with simulated 50 ms work
python3 benchmarks/run.py webhook --jobs 2000
make bench-verify          # check the saved reference report
python3 benchmarks/verify.py benchmarks/local.json
```

The individual commands write benchmarks/local.json. Add `--ledger-dir /tmp/relay-ledgers` to retain the sorted accepted UUID sets for independent inspection. Their SHA-256 digests match the compact report. To collect all scenarios into a new report:

```sh
python3 benchmarks/run.py all --jobs 2000 --chaos-jobs 10000 --seconds 20 --rate 100 --repeats 3 --output benchmarks/local.json
python3 benchmarks/verify.py benchmarks/local.json
```

Each invocation generates a fresh project named relay-bench- followed by 12 random hex digits. The controller ignores the development .env, generates credentials, and uses project-scoped containers, networks and volumes. It removes that project's containers and volumes on exit. For an interrupted controller, use the *exact* printed project name with `make bench-clean PROJECT=relay-bench-0123456789ab`. The cleanup script rejects other name formats. Docker image and build caches remain available for subsequent runs. These hardware-sensitive benchmarks are manual. PR CI continues to run the shorter Compose smoke test and database race suite.

## Measurement contract

- **Warm-up.** Each trial sets its worker count, admits 64 warm-up jobs and waits for the queue to drain. Warm-up is reported separately and excluded from the workload counts. The suite retains earlier jobs to reflect a growing database. The second scaling round reverses the worker order. This reduces ordering bias but cannot eliminate it.
- **API and scaling.** k6 uses 16 virtual users to issue a bounded burst. Each iteration submits one distinct keyed job. Every tenth iteration also replays its request. Every HTTP 201 ID is reconciled against PostgreSQL. HTTP 200 must return the original ID. The no-op handler measures the framework path: HTTP admission, durable writes, claim, initial heartbeat and finalization. No sleep is included. This is an integrated producer/consumer benchmark, so its throughput depends on both.
- **Sustained.** k6 schedules arrivals at a fixed rate for a fixed duration, with 16 preallocated VUs and a ceiling of 64. Replays are additional HTTP calls. The demo.sleep handler spends 50 ms in simulated I/O. **Webhook** trials perform real signed HTTPS requests and durable receiver writes, with a distinct event per measured job. The receiver is local to the Docker network.
- **Measurement and drain.** HTTP request rate uses the client span from the first request start (approximated from its response timestamp and duration) to the last response. Enqueue percentiles use HTTP 201 durations. Admission rate counts only new jobs. End-to-end latency is PostgreSQL completed_at minus created_at, including queue wait. Completion throughput divides successes by the interval from first creation to last completion. Drain time is the remaining interval from last creation to last completion. Rates therefore use different clocks and denominators. Cleanup, startup and k6 container launch are excluded from these rates. Trial elapsed time includes controller work.
- **Sampling.** API database depth is read from one scrape target. Worker counter and gauge readings are aggregated across independent workers in each stable trial. Subtract consecutive counter readings only within a stable process set. The controller samples active handlers, handler duration sum/count, completions, failures, recoveries, HTTP duration sum/count, depth and PostgreSQL lock waiters approximately once per second plus scrape overhead. A Docker stats snapshot supplies point-in-time CPU and memory, not run averages. Sparse samples can miss peaks. The crash trial uses durable rows across restarts.
- **Limits.** The load generator has a four-minute iteration deadline and an eight-second HTTP timeout. Drain polling is bounded to three minutes. Each measured trial must finish within its 300-second budget. Counts and invariants determine pass/fail; there is no minimum throughput threshold. Short local bursts, shared CPU with the load generator, autovacuum, OS caches and row accumulation affect comparisons.

## Crash protocol and verification

The controller first sends a four-second job, observes its running state, and requests SIGTERM. The worker must finish it in one attempt. With workers stopped, the controller admits the burst plus a high-priority signed webhook and a deliberately failing one-attempt job. It retains the UUIDs returned by successful submissions. A test-only PostgreSQL trigger counts accepted deliveries at the receiver, including conflicts on the deduplication key. A second trigger parks the webhook's first completion before commit.

The controller observes the parked database backend and committed receiver row, kills the benchmark workers with SIGKILL, and terminates any remaining parked backend so its transaction rolls back immediately. It starts replacements and lets the ordinary lease, reaper and backoff logic finish. The test uses observed conditions, with no random choice of victim or kill delay. Random project names and keys isolate runs.

The verifier compares the set of returned UUIDs with the durable rows for the run; the report retains a digest of the sorted accepted IDs. It also checks attempt numbering, budgets, final outcomes and absence of overlapping recorded attempts. Expired claim holders cannot finalize through the queue's token and lease checks; the existing PostgreSQL queue tests exercise those rejected operations directly. The webhook crash trial requires an expired first attempt, a successful second attempt, two receiver accepts and one effect. Extra claims are counted from attempt history. Only receiver evidence establishes repeated execution of the external effect path. A terminal dead job still counts as accounted for.

