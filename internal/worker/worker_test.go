package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amgazal/relay-queue/internal/queue"
	"github.com/amgazal/relay-queue/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func settings(id string) Config {
	return Config{ID: id, Concurrency: 1, Lease: 3 * time.Second, Poll: 20 * time.Millisecond, Reap: 100 * time.Millisecond, Timeout: time.Minute, Grace: 5 * time.Second}
}

func start(t *testing.T, p *pgxpool.Pool, c Config, h Registry) (context.CancelFunc, chan struct{}) {
	t.Helper()
	w, err := New(queue.New(p), c, h, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	t.Cleanup(func() { stop(); await(t, done) })
	return stop, done
}
func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for worker")
	}
}
func eventually(t *testing.T, p *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		var ok bool
		if err := p.QueryRow(ctx, sql, args...).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("condition timed out: %s", sql)
}
func enqueue(t *testing.T, p *pgxpool.Pool, kind string, max int32, payload string) string {
	t.Helper()
	j, err := queue.New(p).Enqueue(t.Context(), queue.Submission{Kind: kind, Payload: []byte(payload), MaxAttempts: max})
	if err != nil {
		t.Fatal(err)
	}
	return j.ID
}

func TestHandlerErrors(t *testing.T) {
	h := Registry{"panic": func(context.Context, json.RawMessage) error { panic("boom") }}
	for kind, want := range map[string]string{"missing": "unregistered", "panic": "handler panic: boom"} {
		err := invoke(t.Context(), h, queue.Job{Kind: kind})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatal(err)
		}
	}
}

func TestFailures(t *testing.T) {
	p := testdb.Open(t)
	c := settings("errors")
	c.Timeout = 200 * time.Millisecond
	var attempts atomic.Int32
	h := Registry{
		"retry": func(context.Context, json.RawMessage) error {
			if attempts.Add(1) == 1 {
				return errors.New("retry me")
			}
			return nil
		},
		"timeout": func(ctx context.Context, _ json.RawMessage) error { <-ctx.Done(); return nil },
		"panic":   func(context.Context, json.RawMessage) error { panic("boom") },
	}
	for _, k := range []string{"retry", "panic", "timeout", "unknown"} {
		max := int32(1)
		if k == "retry" {
			max = 2
		}
		enqueue(t, p, k, max, "null")
	}
	start(t, p, c, h)
	eventually(t, p, `SELECT count(*) FILTER (WHERE state='dead')=3 AND count(*) FILTER (WHERE kind='retry' AND state='succeeded' AND attempt_count=2)=1 FROM jobs`)
	eventually(t, p, `SELECT count(DISTINCT error_message)=4 FROM job_attempts WHERE error_message IN ('retry me','handler panic: boom','context deadline exceeded','unregistered job type "unknown"')`)
}

func TestDrainKeepsLease(t *testing.T) {
	p := testdb.Open(t)
	c := settings("drain")
	id := enqueue(t, p, "wait", 1, "null")
	entered, release := make(chan struct{}), make(chan struct{})
	stop, done := start(t, p, c, Registry{"wait": func(ctx context.Context, _ json.RawMessage) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	await(t, entered)
	stop()
	var before time.Time
	if err := p.QueryRow(t.Context(), "SELECT lease_expires_at FROM jobs WHERE id=$1", id).Scan(&before); err != nil {
		t.Fatal(err)
	}
	enqueue(t, p, "wait", 1, "null")
	eventually(t, p, "SELECT lease_expires_at>$2 FROM jobs WHERE id=$1", id, before)
	close(release)
	await(t, done)
	eventually(t, p, "SELECT count(*) FILTER (WHERE state='succeeded')=1 AND count(*) FILTER (WHERE state='queued')=1 FROM jobs")
}

func TestAbandonment(t *testing.T) {
	for _, forced := range []bool{true, false} {
		t.Run(fmt.Sprint("forced-", forced), func(t *testing.T) {
			p := testdb.Open(t)
			c := settings("old")
			if forced {
				c.Grace = 150 * time.Millisecond
			} else {
				c.Grace = 30 * time.Second
			}
			c.Reap = time.Hour
			id := enqueue(t, p, "wait", 2, "null")
			entered, cancelled := make(chan struct{}), make(chan struct{})
			stop, done := start(t, p, c, Registry{"wait": func(ctx context.Context, _ json.RawMessage) error {
				close(entered)
				<-ctx.Done()
				close(cancelled)
				return nil
			}})
			await(t, entered)
			expire := func() {
				_, err := p.Exec(t.Context(), "UPDATE jobs SET lease_expires_at=now()-interval '1 hour' WHERE id=$1", id)
				if err != nil {
					t.Fatal(err)
				}
			}
			if forced {
				stop()
				await(t, done)
				await(t, cancelled)
				expire()
			} else {
				// Drain claims before expiring the active lease so only the replacement can retry.
				stop()
				expire()
				await(t, cancelled)
				await(t, done)
			}
			eventually(t, p, "SELECT state IN ('running','queued') AND attempt_count=1 AND completed_at IS NULL FROM jobs WHERE id=$1", id)
			start(t, p, settings("replacement"), Registry{"wait": func(context.Context, json.RawMessage) error { return nil }})
			eventually(t, p, "SELECT state='succeeded' AND attempt_count=2 FROM jobs WHERE id=$1", id)
			eventually(t, p, "SELECT array_agg(outcome ORDER BY attempt_number)=ARRAY['expired','succeeded'] FROM job_attempts WHERE job_id=$1", id)
		})
	}
}

func TestTwoWorkers1000(t *testing.T) {
	p := testdb.Open(t)
	var calls [1000]atomic.Int32
	for i := range calls {
		enqueue(t, p, "count", 2, fmt.Sprint(i))
	}
	gate := make(chan struct{})
	ready := make(chan struct{}, 6)
	deferGate, cancelGate := context.WithCancel(t.Context())
	defer cancelGate()
	for i := range 2 {
		c := settings(fmt.Sprint("worker-", i))
		c.Concurrency = 3
		var active atomic.Int32
		h := func(ctx context.Context, payload json.RawMessage) error {
			n := active.Add(1)
			defer active.Add(-1)
			if n > 3 {
				return fmt.Errorf("active handlers %d", n)
			}
			var id int
			if err := json.Unmarshal(payload, &id); err != nil {
				return err
			}
			calls[id].Add(1)
			select {
			case <-gate:
				return nil
			default:
			}
			ready <- struct{}{}
			select {
			case <-gate:
				return nil
			case <-deferGate.Done():
				return deferGate.Err()
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		instance, err := pgxpool.NewWithConfig(t.Context(), p.Config())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(instance.Close)
		start(t, instance, c, Registry{"count": h})
	}
	for range 6 {
		await(t, ready)
	}
	close(gate)
	eventually(t, p, "SELECT count(*) FILTER (WHERE state='succeeded')=1000 FROM jobs")
	eventually(t, p, "SELECT count(*)=1000 AND count(DISTINCT lock_token)=1000 AND count(DISTINCT job_id)=1000 AND count(DISTINCT worker_id)=2 AND bool_and(outcome='succeeded') FROM job_attempts")
	for id := range calls {
		if got := calls[id].Load(); got != 1 {
			t.Fatalf("job %d invoked %d times", id, got)
		}
	}
}
