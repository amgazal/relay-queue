# Reliable webhook delivery

A webhook lets an application notify another system when an event happens. Submitting a durable Relay job decouples the producer from receiver availability. The worker sends an event using the same leases, timeouts, retries and attempt history as other jobs.

## Run the local demo

Run from the repository root with Go, Docker, Make and OpenSSL. These commands assume the default Compose credentials and free local ports. Start all three processes from the same shell so they inherit the configuration.

```sh
make migrate build
export RELAY_DATABASE_URL=postgres://relay:relay_local@localhost:5432/relay
export RELAY_API_KEY="$(openssl rand -hex 32)"
export RELAY_DEMO_SIGNING_SECRET="$(openssl rand -hex 32)"
export RELAY_WEBHOOK_DESTINATIONS="{\"local-demo\":{\"url\":\"http://127.0.0.1:8082/events\",\"secret\":\"$RELAY_DEMO_SIGNING_SECRET\"}}"
export RELAY_DEMO_FAIL_FIRST=true
./bin/relay-demo-receiver &
./bin/relay-worker &
./bin/relay-api &
curl --retry 5 --retry-connrefused -fsS http://127.0.0.1:8080/readyz
```

Submit the same logical event twice. These submissions omit an API idempotency key intentionally, so each creates a job. With fail-first enabled, the receiver returns 503 for its first valid request per process. Subsequent valid deliveries return 204 once the inbox write commits.

```sh
event_id=$(uuidgen | tr '[:upper:]' '[:lower:]')
job="{\"type\":\"webhook.deliver\",\"payload\":{\"destination\":\"local-demo\",\"event_id\":\"$event_id\",\"event_type\":\"ticket.resolved\",\"data\":{\"ticket_id\":\"TICKET-123\",\"status\":\"resolved\"}}}"
for delivery in 1 2; do
  curl -fsS -H "Authorization: Bearer $RELAY_API_KEY" -H 'Content-Type: application/json' \
    -d "$job" http://127.0.0.1:8080/v1/jobs
done
curl -fsS -H "Authorization: Bearer $RELAY_API_KEY" 'http://127.0.0.1:8080/v1/jobs?type=webhook.deliver'
docker compose exec -T db psql -U relay -d relay -c "SELECT event_type,data,accepted_at FROM demo_received_events WHERE event_id='$event_id'"
```

Inspect after the worker finishes retrying. The two jobs should succeed, with one inbox row for the event. To reproduce the ownership-loss case without waiting for real lease deadlines:

```sh
RELAY_TEST_DATABASE_URL="$RELAY_DATABASE_URL" go test -race -count=1 -v ./internal/webhook -run TestCrashAfterAcceptance
```

The test withholds the HTTP acknowledgment after a real receiver commit, forces worker shutdown, expires its abandoned lease, and starts a replacement worker. It checks that stale completion fails, the original attempt is recorded as expired, the retry succeeds, and two accepted HTTP requests yield one durable effect.

## Event and destination contract

The job payload has exactly four fields. Destination and event_type are 1–128 ASCII letters, digits, dots, underscores or hyphens, starting with an alphanumeric character. event_id is a lowercase hyphenated UUID chosen once by the producer. data is any JSON value. The signed wire object contains only event_id, event_type and data. The generic submission API accepts jobs before handler-specific validation; an invalid webhook payload becomes a permanent execution failure.

Configure each worker with the same JSON destination map. Each entry supplies a trusted URL and a signing secret of at least 32 bytes. An unset map permits ordinary workers to start; a webhook naming an unavailable destination fails permanently. Secrets are literal strings: the hexadecimal string produced by OpenSSL is used as ASCII key bytes on both sides. Keep configuration outside source control.

## Signature verification

The worker sends POST with Content-Type application/json, X-Relay-Event-ID, X-Relay-Timestamp (Unix seconds in decimal), and X-Relay-Signature (`sha256=` followed by lowercase hex). Sign exactly the transmitted bytes, including whitespace in the body. Each attempt signs again with its current timestamp.

```go
// stamp and id are the received header strings. raw is the bounded, unreadjusted body.
prefix, hexMAC, ok := strings.Cut(signatureHeader, "=")
received, err := hex.DecodeString(hexMAC)
mac := hmac.New(sha256.New, []byte(secret))
io.WriteString(mac, stamp+"."+id+".")
mac.Write(raw)
valid := ok && prefix == "sha256" && err == nil && hmac.Equal(received, mac.Sum(nil))
```

The demo receiver checks a five-minute window in both directions, verifies the MAC with constant-time comparison, then strictly parses the event and matches its ID to the signed header. Sender and receiver clocks must agree. The receiver bounds bodies at 1 MiB. The inbox row is the business effect: PostgreSQL's unique event ID makes concurrent insertions and deliveries after a restart safe. Reusing an ID with different data preserves the first accepted event.

## Delivery policy

| Observation | Worker behavior |
| --- | --- |
| HTTP 2xx | Record success. This confirms the receiver acknowledged the request; downstream actions may still be pending. |
| Network error, timeout, 408, 429, 5xx | Use the existing exponential backoff and attempt budget; exhausted jobs become dead. |
| Other HTTP status, including redirects and other 4xx | Record the failed attempt and mark the job dead immediately under its current lease. |
| Invalid payload or unknown destination | Same permanent-failure transition, without sending a request. |

API submission idempotency prevents duplicate **job rows** for identical keyed submissions. A per-claim token protects Relay's **job state** from stale workers. The stable event ID protects the **receiver's effect** across different attempts or jobs. Losing ownership or cancelling a request cannot undo a receiver commit. External systems with additional side effects need their own atomic deduplication strategy.

## Security and operations

Only trusted configuration supplies complete destination URLs. Job input selects a map entry and never contributes to the URL. The sender disables redirects and proxy lookup, requires HTTPS except for literal loopback HTTP addresses, rejects URL credentials, query strings and fragments, and reads at most 4 KiB of response data. Connection and TLS handshake timeouts are three seconds, the response-header timeout is five seconds, and the request timeout is ten seconds or the earlier handler deadline. Response bodies and URLs are omitted from job errors and logs.

Plain HTTP is for trusted local testing. Restrict production egress and control configured hostnames and DNS. Configuration is loaded on process startup. The demo receiver shares the development database, accepts one signing key, and retains inbox IDs indefinitely. It demonstrates a database-backed effect; it does not provide key rotation, subscription management or transactional delivery to a third system.

Worker metrics include relay_webhook_deliveries_total with bounded outcome labels success, transient and permanent, and relay_webhook_delivery_duration_seconds. Sum the outcomes for completed invocation count; invalid payloads are included. The demo receiver exports relay_webhook_signature_failures_total when RELAY_RECEIVER_METRICS_ADDR is set, for example to 127.0.0.1:9093. Existing worker logs correlate failures to job IDs without recording payloads, signatures or response bodies.
