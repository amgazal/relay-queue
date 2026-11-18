package metrics_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/amgazal/relay-queue/internal/api"
	"github.com/amgazal/relay-queue/internal/metrics"
	"github.com/amgazal/relay-queue/internal/queue"
	"github.com/amgazal/relay-queue/internal/scheduler"
	"github.com/amgazal/relay-queue/internal/testdb"
	"github.com/amgazal/relay-queue/internal/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestObservedLifecycle(t *testing.T) {
	p := testdb.Open(t)
	ctx := t.Context()
	m := metrics.New(p)
	q := queue.WithMetrics(p, m)
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	scrape := func() string {
		r := httptest.NewRecorder()
		promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}).ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
		b, err := io.ReadAll(r.Result().Body)
		must(err)
		return string(b)
	}
	check := func(wants ...string) {
		t.Helper()
		out := scrape()
		for _, want := range wants {
			if !strings.Contains(out, want+"\n") {
				t.Fatal("missing metric", want)
			}
		}
		if strings.Contains(out, "private-marker") {
			t.Fatal("sensitive metric")
		}
	}
	apiHandler, err := api.WithMetrics(p, "private-marker-key", 0, log, m)
	must(err)
	for _, url := range []string{"/v1/private-marker-path", "/healthz"} {
		apiHandler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))
	}
	check(`relay_http_requests_total{route="unmatched",status="401"} 1`, `relay_http_request_duration_seconds_count{route="GET /healthz"} 1`)
	s := scheduler.WithMetrics(p, 0, m)
	schedule, err := s.Create(ctx, scheduler.Input{Name: "name", Cron: "* * * * *", Kind: "private-marker-kind", Payload: json.RawMessage(`"private-marker-payload"`)})
	must(err)
	_, err = p.Exec(ctx, "UPDATE schedules SET next_run=date_trunc('minute',now())-interval '3 minutes' WHERE id=$1", schedule.ID)
	must(err)
	_, err = s.Dispatch(ctx)
	must(err)
	check("relay_scheduler_occurrences_dispatched_total 1", "relay_jobs_submitted_total 1", `relay_queue_depth{state="queued"} 1`, "relay_queue_snapshot_up 1")
	claims, err := q.Claim(ctx, "worker", 1, time.Hour)
	must(err)
	if len(claims) != 1 {
		t.Fatal(claims)
	}
	_, err = p.Exec(ctx, "UPDATE jobs SET lease_expires_at=now()-interval '1 hour'")
	must(err)
	_, err = q.Heartbeat(ctx, claims[0].Ownership, time.Hour)
	if !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatal(err)
	}
	_, err = q.ReapExpired(ctx, 1)
	must(err)
	check("relay_expired_attempts_recovered_total 1", `relay_retries_total{reason="expired"} 1`, "relay_lease_renewal_failures_total 1", "relay_claim_duration_seconds_count 1")
	_, err = p.Exec(ctx, "UPDATE jobs SET available_at=now()")
	must(err)
	entered := make(chan struct{})
	release := make(chan struct{})
	w, err := worker.New(q, worker.Config{ID: "private-marker-worker", Concurrency: 1, Lease: 3 * time.Second, Poll: time.Hour, Reap: time.Hour, Timeout: time.Minute, Grace: time.Second}, worker.Registry{"private-marker-kind": func(context.Context, json.RawMessage) error {
		close(entered)
		<-release
		return errors.New("private-marker-error")
	}}, log)
	must(err)
	active, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); w.Run(active) }()
	defer func() {
		cancel()
		select {
		case <-release:
		default:
			close(release)
		}
		<-done
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not start")
	}
	check("relay_active_handlers 1", "relay_execution_attempts_total 1", `relay_queue_depth{state="running"} 1`)
	cancel()
	close(release)
	<-done
	check("relay_active_handlers 0", "relay_handler_duration_seconds_count 1", "relay_failed_attempts_total 1", `relay_retries_total{reason="failure"} 1`)
	if strings.Contains(logs.String(), "private-marker-error") || strings.Contains(logs.String(), "private-marker-payload") || strings.Contains(logs.String(), "private-marker-key") {
		t.Fatal("sensitive log contents")
	}
	_, err = p.Exec(ctx, "UPDATE jobs SET available_at=now()")
	must(err)
	claims, err = q.Claim(ctx, "worker", 1, time.Hour)
	must(err)
	if len(claims) != 1 {
		t.Fatal(claims)
	}
	must(q.Complete(ctx, claims[0].Ownership))
	check("relay_jobs_completed_total 1", `relay_queue_depth{state="succeeded"} 1`)
	p.Close()
	check("relay_queue_snapshot_up 0")
	if strings.Contains(scrape(), "relay_queue_depth{") {
		t.Fatal("unavailable snapshot exported state")
	}

}
