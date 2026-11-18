package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/amgazal/relay-queue/internal/queue"
	"github.com/amgazal/relay-queue/internal/testdb"
	"github.com/amgazal/relay-queue/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestUTC(t *testing.T) {
	at := time.Date(2028, 2, 28, 23, 59, 59, 0, time.UTC)
	got, err := next("0 0 29 FEB *", at.In(time.FixedZone("east", 7200)))
	if err != nil || !got.Equal(at.Add(time.Second)) {
		t.Fatal(got, err)
	}
	for _, expr := range []string{"@daily", "TZ=Europe/Paris * * * * *", "0 1 2 3 4 5", "* * 31 FEB *", "60 * * * *"} {
		if _, err := next(expr, at); err == nil {
			t.Fatal(expr)
		}
	}
}
func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func fixture(t *testing.T) (*pgxpool.Pool, *Scheduler, Schedule) {
	t.Helper()
	p := testdb.Open(t)
	s := New(p, 0)
	v, err := s.Create(t.Context(), Input{"sample", "0 0 1 JAN *", "demo", json.RawMessage("{}")})
	check(t, err)
	return p, s, v
}
func force(t *testing.T, p *pgxpool.Pool, id string, at time.Time) {
	t.Helper()
	_, err := p.Exec(t.Context(), "UPDATE schedules SET next_run=$1 WHERE id=$2", at, id)
	check(t, err)
}
func jobs(t *testing.T, p *pgxpool.Pool, id string) int {
	t.Helper()
	var n int
	check(t, p.QueryRow(t.Context(), "SELECT count(*) FROM jobs WHERE schedule_id=$1", id).Scan(&n))
	return n
}
func TestConcurrentOccurrences(t *testing.T) {
	p, s, v := fixture(t)
	ctx := t.Context()
	replica, err := pgxpool.NewWithConfig(ctx, p.Config())
	check(t, err)
	defer replica.Close()
	other := New(replica, 0)
	stored, err := other.Get(ctx, v.ID)
	check(t, err)
	if !stored.Next.Equal(v.Next) {
		t.Fatal(stored)
	}
	old := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, expected := range []int{1, 1, 2} {
		due := old
		if i == 2 {
			due = old.AddDate(1, 0, 0)
		}
		force(t, p, v.ID, due)
		gate := make(chan struct{})
		var wg sync.WaitGroup
		for _, runner := range []*Scheduler{s, other} {
			wg.Go(func() {
				<-gate
				_, err := runner.Dispatch(ctx)
				if err != nil {
					t.Error(err)
				}
			})
		}
		close(gate)
		wg.Wait()
		if n := jobs(t, p, v.ID); n != expected {
			t.Fatalf("round %d: %d jobs", i, n)
		}
		current, err := s.Get(ctx, v.ID)
		check(t, err)
		if !current.Next.Equal(v.Next) {
			t.Fatal("missed executions did not advance to future match", current)
		}
	}
	force(t, p, v.ID, old.AddDate(2, 0, 0))
	var wg sync.WaitGroup
	wg.Go(func() {
		_, err := other.Disable(ctx, v.ID)
		if err != nil {
			t.Error(err)
		}
	})
	_, err = s.Dispatch(ctx)
	check(t, err)
	wg.Wait()
	n := jobs(t, p, v.ID)
	if n < 2 || n > 3 {
		t.Fatal(n)
	}
	force(t, p, v.ID, old.AddDate(3, 0, 0))
	ok, err := other.Dispatch(ctx)
	check(t, err)
	if ok || jobs(t, p, v.ID) != n {
		t.Fatal("disabled schedule dispatched")
	}
}
func TestRollbackRecoveryAndWorker(t *testing.T) {
	p, s, v := fixture(t)
	ctx := t.Context()
	old := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	force(t, p, v.ID, old)
	// Fail the first two advances, after the job has been inserted. Sequence increments survive rollback.
	_, err := p.Exec(ctx, `CREATE SEQUENCE faults;
 CREATE FUNCTION interrupt_advance() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF nextval('faults')<=2 THEN RAISE EXCEPTION 'temporary failure'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER interrupt BEFORE UPDATE ON schedules FOR EACH ROW EXECUTE FUNCTION interrupt_advance()`)
	check(t, err)
	_, err = s.Dispatch(ctx)
	if err == nil {
		t.Fatal("expected failed advancement")
	}
	current, err := s.Get(ctx, v.ID)
	check(t, err)
	if !current.Next.Equal(old) || jobs(t, p, v.ID) != 0 {
		t.Fatal("partial dispatch")
	}
	log := slog.New(slog.DiscardHandler)
	w, err := worker.New(queue.New(p), worker.Config{ID: "worker", Concurrency: 1, Lease: 3 * time.Second, Timeout: time.Minute, Poll: 10 * time.Millisecond, Reap: time.Minute, Grace: time.Second}, worker.Registry{"demo": func(context.Context, json.RawMessage) error { return nil }}, log)
	check(t, err)
	active, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { s.Run(active, 10*time.Millisecond, log) })
	wg.Go(func() { w.Run(active) })
	defer func() {
		stop()
		ch := make(chan struct{})
		go func() { wg.Wait(); close(ch) }()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Error("shutdown timed out")
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var done bool
		check(t, p.QueryRow(ctx, `SELECT EXISTS(SELECT FROM jobs j JOIN job_attempts a ON a.job_id=j.id WHERE schedule_id=$1 AND occurrence_at=$2 AND j.state='succeeded' AND a.outcome='succeeded')`, v.ID, old).Scan(&done))
		if done {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("worker did not complete scheduled job")
}

func TestSkippedMetricsCount(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, expr := range []string{"* * * * *", "0,30 8-10 * * MON-FRI", "0 0 29 FEB *"} {
		end := start.AddDate(0, 2, 0)
		sched, err := parser.Parse(expr)
		check(t, err)
		var want int64
		for at := sched.Next(start); !at.After(end); at = sched.Next(at) {
			want++
		}
		if got := skipped(expr, start, end); got != want {
			t.Fatal(expr, got, want)
		}
	}
}
