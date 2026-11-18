package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRetryDelay(t *testing.T) {
	for _, tc := range []struct {
		attempt int32
		ceiling time.Duration
	}{{1, time.Second}, {2, 2 * time.Second}, {9, 256 * time.Second}, {10, 5 * time.Minute}, {2147483647, 5 * time.Minute}} {
		for _, sample := range []float64{0, 0.5, 0.999999} {
			got := retryDelay(tc.attempt, sample)
			if got < tc.ceiling/2 || got > tc.ceiling {
				t.Fatalf("attempt %d: delay %v", tc.attempt, got)
			}
		}
	}
}

func database(t *testing.T) *Queue {
	t.Helper()
	url := os.Getenv("RELAY_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set RELAY_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	check(t, err)
	t.Cleanup(admin.Close)
	schema := pgx.Identifier{fmt.Sprintf("relay_test_%d", time.Now().UnixNano())}.Sanitize()
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+schema)
	check(t, err)
	t.Cleanup(func() {
		_, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		if err != nil {
			t.Error(err)
		}
	})
	config := admin.Config()
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	check(t, err)
	t.Cleanup(pool.Close)
	files, err := filepath.Glob("../../migrations/*.sql")
	check(t, err)
	for _, file := range files {
		sql, err := os.ReadFile(file)
		check(t, err)
		_, err = pool.Exec(ctx, string(sql))
		check(t, err)
	}
	return New(pool)
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func submit(t *testing.T, q *Queue, s Submission) Job {
	t.Helper()
	j, err := q.Enqueue(t.Context(), s)
	check(t, err)
	return j
}
func claim(t *testing.T, q *Queue) Claim {
	t.Helper()
	claims, err := q.Claim(t.Context(), "worker", 1, time.Hour)
	check(t, err)
	if len(claims) != 1 {
		t.Fatalf("got %d claims", len(claims))
	}
	return claims[0]
}
func exec(t *testing.T, q *Queue, sql string, args ...any) {
	t.Helper()
	_, err := q.pool.Exec(t.Context(), sql, args...)
	check(t, err)
}
func parallel(n int, fn func(int) error) error {
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, n)
	for i := range n {
		wg.Go(func() { <-start; errs <- fn(i) })
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func TestConcurrentClaims(t *testing.T) {
	q := database(t)
	ctx := t.Context()
	for range 100 {
		submit(t, q, Submission{Kind: "send", Payload: []byte("null")})
	}
	ids := make(chan string, 100)
	check(t, parallel(50, func(i int) error {
		claims, err := q.Claim(ctx, fmt.Sprint(i), 2, time.Hour)
		if err != nil {
			return err
		}
		if len(claims) != 2 {
			return fmt.Errorf("worker %d claimed %d jobs", i, len(claims))
		}
		for _, c := range claims {
			ids <- c.Job.ID
		}
		return nil
	}))
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate claim %s", id)
		}
		seen[id] = true
	}
	var histories int
	check(t, q.pool.QueryRow(ctx, "SELECT count(*) FROM job_attempts WHERE attempt_number=1").Scan(&histories))
	if histories != 100 {
		t.Fatal(histories)
	}
}

func TestOrdering(t *testing.T) {
	q := database(t)
	s := Submission{Kind: "send", Payload: []byte("{}")}
	low := submit(t, q, s)
	s.Priority = 5
	high := submit(t, q, s)
	future := time.Now().Add(24 * time.Hour)
	s.ScheduledAt = &future
	s.Priority = 10
	submit(t, q, s)
	claims, err := q.Claim(t.Context(), "worker", 10, time.Hour)
	check(t, err)
	if len(claims) != 2 || claims[0].Job.ID != high.ID || claims[1].Job.ID != low.ID {
		t.Fatal(claims)
	}
}

func TestIdempotencyAndFailure(t *testing.T) {
	q := database(t)
	ctx := t.Context()
	key := "order-7"
	s := Submission{Kind: "send", Payload: []byte(`{"a":1,"b":2}`), IdempotencyKey: &key, MaxAttempts: 2}
	ids := make([]string, 30)
	check(t, parallel(len(ids), func(i int) error { j, err := q.Enqueue(ctx, s); ids[i] = j.ID; return err }))
	for _, id := range ids {
		if id != ids[0] {
			t.Fatal(ids)
		}
	}
	c := claim(t, q)
	check(t, q.Fail(ctx, c.Ownership, "timeout"))
	s.Payload = []byte(`{"b":2,"a":1}`)
	j := submit(t, q, s)
	if j.ID != c.Job.ID || j.State != "queued" || j.AttemptCount != 1 {
		t.Fatal(j)
	}
	var delay time.Duration
	var micros int64
	check(t, q.pool.QueryRow(ctx, "SELECT (extract(epoch FROM available_at-updated_at)*1000000)::bigint FROM jobs WHERE id=$1", j.ID).Scan(&micros))
	delay = time.Duration(micros) * time.Microsecond
	if delay < 499*time.Millisecond || delay > 1100*time.Millisecond {
		t.Fatal(delay)
	}
	future := time.Now().Add(time.Hour)
	for _, change := range []func(*Submission){func(v *Submission) { v.Priority++ }, func(v *Submission) { v.Kind = "other" }, func(v *Submission) { v.Payload = []byte("null") }, func(v *Submission) { v.MaxAttempts++ }, func(v *Submission) { v.ScheduledAt = &future }} {
		other := s
		change(&other)
		_, err := q.Enqueue(ctx, other)
		if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	exec(t, q, "UPDATE jobs SET available_at=now() WHERE id=$1", j.ID)
	next := claim(t, q)
	if next.Ownership.Token == c.Ownership.Token {
		t.Fatal("reused token")
	}
	if err := q.Complete(ctx, c.Ownership); err != ErrLeaseLost {
		t.Fatal(err)
	}
	check(t, q.Fail(ctx, next.Ownership, "permanent"))
	j = submit(t, q, s)
	if j.State != "dead" || j.AttemptCount != 2 {
		t.Fatal(j)
	}
	var history []string
	check(t, q.pool.QueryRow(ctx, "SELECT array_agg(error_message ORDER BY attempt_number) FROM job_attempts WHERE outcome='failed' AND finished_at IS NOT NULL").Scan(&history))
	if fmt.Sprint(history) != "[timeout permanent]" {
		t.Fatal(history)
	}
}

func TestRecovery(t *testing.T) {
	q := database(t)
	ctx := t.Context()
	s := Submission{Kind: "send", Payload: []byte("{}"), MaxAttempts: 2}
	job := submit(t, q, s)
	c := claim(t, q)
	wrong := c.Ownership
	wrong.Worker = "wrong"
	if err := q.Complete(ctx, wrong); err != ErrLeaseLost {
		t.Fatal(err)
	}
	deadline, err := q.Heartbeat(ctx, c.Ownership, 2*time.Hour)
	check(t, err)
	if !deadline.After(c.LeaseExpiresAt) {
		t.Fatal(deadline)
	}
	s.MaxAttempts = 1
	doomed := submit(t, q, s)
	claim(t, q)
	exec(t, q, "UPDATE jobs SET lease_expires_at=now()-interval '1 hour'")
	if err = q.Complete(ctx, c.Ownership); err != ErrLeaseLost {
		t.Fatal(err)
	}
	if err = q.Fail(ctx, c.Ownership, "late"); err != ErrLeaseLost {
		t.Fatal(err)
	}
	if _, err = q.Heartbeat(ctx, c.Ownership, time.Hour); err != ErrLeaseLost {
		t.Fatal(err)
	}
	counts := make([]int, 8)
	check(t, parallel(len(counts), func(i int) error { n, err := q.ReapExpired(ctx, 1); counts[i] = n; return err }))
	sum := 0
	for _, n := range counts {
		if n > 1 {
			t.Fatal(n)
		}
		sum += n
	}
	if sum != 2 {
		t.Fatal(counts)
	}
	var dead bool
	check(t, q.pool.QueryRow(ctx, "SELECT state='dead' AND lock_token IS NULL FROM jobs WHERE id=$1", doomed.ID).Scan(&dead))
	if !dead {
		t.Fatal("exhausted job survived")
	}
	exec(t, q, "UPDATE jobs SET available_at=now() WHERE id=$1", job.ID)
	next := claim(t, q)
	if err = q.Fail(ctx, c.Ownership, "stale"); err != ErrLeaseLost {
		t.Fatal(err)
	}
	if _, err = q.Heartbeat(ctx, c.Ownership, time.Hour); err != ErrLeaseLost {
		t.Fatal(err)
	}
	check(t, q.Complete(ctx, next.Ownership))
	var ok bool
	check(t, q.pool.QueryRow(ctx, "SELECT state='succeeded' AND completed_at IS NOT NULL AND lease_owner IS NULL FROM jobs WHERE id=$1", job.ID).Scan(&ok))
	if !ok {
		t.Fatal("incomplete success transition")
	}
	var history []string
	check(t, q.pool.QueryRow(ctx, "SELECT array_agg(outcome ORDER BY attempt_number) FROM job_attempts WHERE job_id=$1 AND finished_at IS NOT NULL", job.ID).Scan(&history))
	if fmt.Sprint(history) != "[expired succeeded]" {
		t.Fatal(history)
	}
}
