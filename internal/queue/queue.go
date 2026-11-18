// Package queue implements durable job transitions in PostgreSQL.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/amgazal/relay-queue/internal/metrics"
	"math"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrInvalid   Error = "invalid queue input"
	ErrConflict  Error = "conflicting idempotency key"
	ErrLeaseLost Error = "claim is no longer valid"
)

type Queue struct {
	pool    *pgxpool.Pool
	Metrics *metrics.Metrics
}

func New(pool *pgxpool.Pool) *Queue { return WithMetrics(pool, metrics.New(nil)) }

func WithMetrics(pool *pgxpool.Pool, m *metrics.Metrics) *Queue { return &Queue{pool, m} }

type Submission struct {
	Kind           string
	Payload        json.RawMessage
	Priority       int32
	ScheduledAt    *time.Time
	MaxAttempts    int32 // Zero selects five attempts.
	IdempotencyKey *string
}

type Job struct {
	ID, Kind, State                     string
	Payload                             json.RawMessage
	Priority, AttemptCount, MaxAttempts int32
	AvailableAt                         time.Time
}

type Ownership struct{ JobID, Worker, Token string }
type Claim struct {
	Job            Job
	Ownership      Ownership
	LeaseExpiresAt time.Time
}

const fields = "j.id::text,j.kind,j.state,j.payload,j.priority,j.attempt_count,j.max_attempts,j.available_at"

func (j *Job) columns() []any {
	return []any{&j.ID, &j.Kind, &j.State, &j.Payload, &j.Priority, &j.AttemptCount, &j.MaxAttempts, &j.AvailableAt}
}

func (q *Queue) Enqueue(ctx context.Context, s Submission) (Job, error) {
	j, _, err := q.Submit(ctx, s, 0, 0)
	return j, err
}

// Submit adds relative scheduling and optional active-job admission control.
func (q *Queue) Submit(ctx context.Context, s Submission, delay int32, limit int) (Job, bool, error) {
	var j Job
	var created bool
	err := pgx.BeginTxFunc(ctx, q.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		var err error
		j, created, err = SubmitTx(ctx, tx, s, delay, limit)
		return err
	})
	if errors.Is(err, ErrConflict) {
		q.Metrics.Conflicts.Inc()
	}
	if err != nil {
		return Job{}, false, err
	}
	if created {
		q.Metrics.Submitted.Inc()
	}
	return j, created, nil
}

// SubmitTx submits within the caller's READ COMMITTED transaction. Roll back on error.
func SubmitTx(ctx context.Context, tx pgx.Tx, s Submission, delay int32, limit int) (Job, bool, error) {
	if delay < 0 || limit < 0 || (s.ScheduledAt != nil && delay != 0) {
		return Job{}, false, ErrInvalid
	}

	if strings.TrimSpace(s.Kind) == "" || !json.Valid(s.Payload) || s.MaxAttempts < 0 || (s.IdempotencyKey != nil && strings.TrimSpace(*s.IdempotencyKey) == "") {
		return Job{}, false, ErrInvalid
	}
	if s.MaxAttempts == 0 {
		s.MaxAttempts = 5
	}
	var j Job

	if err := admissionLock(ctx, tx, limit); err != nil {
		return Job{}, false, err
	}
	args := []any{s.Kind, s.Payload, s.Priority, s.ScheduledAt, s.MaxAttempts, s.IdempotencyKey, delay}
	err := tx.QueryRow(ctx, `INSERT INTO jobs AS j(kind,payload,priority,requested_at,available_at,max_attempts,idempotency_key,delay_seconds)
 VALUES($1,$2,$3,$4,coalesce($4,now()+$7::integer * interval '1 second'),$5,$6,$7) ON CONFLICT(idempotency_key) DO NOTHING RETURNING `+fields, args...).Scan(j.columns()...)
	if err == nil {
		return j, true, checkCapacity(ctx, tx, limit)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, err
	}
	// A fresh READ COMMITTED snapshot sees the winning insert after a concurrent conflict.
	var same bool
	err = tx.QueryRow(ctx, `SELECT `+fields+`,kind=$1 AND payload=$2 AND priority=$3 AND requested_at IS NOT DISTINCT FROM $4 AND coalesce(original_max_attempts,max_attempts)=$5 AND delay_seconds=$7
 FROM jobs j WHERE idempotency_key=$6`, args...).Scan(append(j.columns(), &same)...)
	if err == nil && !same {
		return Job{}, false, ErrConflict
	}
	return j, false, err
}

func (q *Queue) Claim(ctx context.Context, worker string, limit int, lease time.Duration) ([]Claim, error) {
	start := time.Now()
	defer func() { q.Metrics.ClaimTime.Observe(time.Since(start).Seconds()) }()
	if strings.TrimSpace(worker) == "" || limit < 1 || lease.Microseconds() < 1 {
		return nil, ErrInvalid
	}
	// The history insert consumes the updated rows, so both writes succeed or roll back together.
	rows, err := q.pool.Query(ctx, `WITH candidates AS (
 SELECT id FROM jobs WHERE state='queued' AND available_at<=statement_timestamp() AND attempt_count<max_attempts
 ORDER BY priority DESC,available_at,id LIMIT $1 FOR UPDATE SKIP LOCKED
 ), claimed AS (
 UPDATE jobs j SET state='running',attempt_count=attempt_count+1,lease_owner=$2,lock_token=gen_random_uuid(),
 lease_expires_at=clock_timestamp()+$3::bigint * interval '1 microsecond', updated_at=clock_timestamp()
 FROM candidates c WHERE j.id=c.id RETURNING j.*
 ), history AS (
 INSERT INTO job_attempts(job_id,attempt_number,worker_id,lock_token)
 SELECT id,attempt_count,lease_owner,lock_token FROM claimed RETURNING job_id
 ) SELECT `+fields+`,j.lock_token::text,j.lease_expires_at FROM claimed j JOIN history h ON h.job_id=j.id
 ORDER BY j.priority DESC,j.available_at,j.id`, limit, worker, lease.Microseconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Claim, error) {
		var c Claim
		err := row.Scan(append(c.Job.columns(), &c.Ownership.Token, &c.LeaseExpiresAt)...)
		c.Ownership.JobID, c.Ownership.Worker = c.Job.ID, worker
		return c, err
	})
}

// Lock first so a caller waiting behind another transaction checks the lease against fresh database time.
func (q *Queue) owned(ctx context.Context, o Ownership, fn func(pgx.Tx, int32, int32) error) error {
	var uuid pgtype.UUID
	if uuid.Scan(o.JobID) != nil || uuid.Scan(o.Token) != nil {
		return ErrLeaseLost
	}
	return pgx.BeginFunc(ctx, q.pool, func(tx pgx.Tx) error {
		var n, max int32
		err := tx.QueryRow(ctx, `SELECT attempt_count,max_attempts FROM jobs WHERE id=$1 AND lease_owner=$2 AND lock_token=$3 AND state='running' FOR UPDATE`, o.JobID, o.Worker, o.Token).Scan(&n, &max)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		var live bool
		if err = tx.QueryRow(ctx, "SELECT lease_expires_at>clock_timestamp() FROM jobs WHERE id=$1", o.JobID).Scan(&live); err != nil {
			return err
		}
		if !live {
			return ErrLeaseLost
		}
		return fn(tx, n, max)
	})
}

func (q *Queue) Heartbeat(ctx context.Context, o Ownership, lease time.Duration) (time.Time, error) {
	if lease.Microseconds() < 1 {
		return time.Time{}, ErrInvalid
	}
	var deadline time.Time
	err := q.owned(ctx, o, func(tx pgx.Tx, _, _ int32) error {
		return tx.QueryRow(ctx, `UPDATE jobs SET lease_expires_at=greatest(lease_expires_at,clock_timestamp()+$2::bigint * interval '1 microsecond'),updated_at=clock_timestamp() WHERE id=$1 RETURNING lease_expires_at`, o.JobID, lease.Microseconds()).Scan(&deadline)
	})
	if err != nil {
		q.Metrics.RenewFailures.Inc()
	}
	return deadline, err
}

func (q *Queue) Complete(ctx context.Context, o Ownership) error {
	err := q.owned(ctx, o, func(tx pgx.Tx, n, max int32) error { return finish(ctx, tx, o, n, max, "succeeded", nil) })
	if err == nil {
		q.Metrics.Completed.Inc()
	}
	return err
}
func (q *Queue) Fail(ctx context.Context, o Ownership, message string) error {
	retry := false
	err := q.owned(ctx, o, func(tx pgx.Tx, n, max int32) error {
		retry = n < max
		return finish(ctx, tx, o, n, max, "failed", &message)
	})
	if err == nil {
		q.Metrics.Failed.Inc()
		if retry {
			q.Metrics.Retries.WithLabelValues("failure").Inc()
		}
	}
	return err
}

// Equal jitter chooses between half and all of a capped exponential delay.
func retryDelay(n int32, sample float64) time.Duration {
	ceiling := min(5*time.Minute, time.Second<<min(max(n-1, 0), 9))
	return ceiling/2 + time.Duration(math.Floor(float64(ceiling/2)*sample))
}

func finish(ctx context.Context, tx pgx.Tx, o Ownership, n, max int32, outcome string, message *string) error {
	tag, err := tx.Exec(ctx, `UPDATE job_attempts SET outcome=$4,error_message=$5,finished_at=clock_timestamp()
 WHERE job_id=$1 AND attempt_number=$2 AND lock_token=$3 AND outcome='running'`, o.JobID, n, o.Token, outcome, message)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("missing open attempt for job %s attempt %d", o.JobID, n)
	}
	state := "succeeded"
	var delay time.Duration
	if outcome != "succeeded" {
		state = "dead"
		if n < max {
			state = "queued"
			delay = retryDelay(n, rand.Float64())
		}
	}
	_, err = tx.Exec(ctx, `UPDATE jobs SET state=$2,lease_owner=NULL,lock_token=NULL,lease_expires_at=NULL,updated_at=clock_timestamp(),
 completed_at=CASE WHEN $2='succeeded' THEN clock_timestamp() END,
 available_at=CASE WHEN $2='queued' THEN clock_timestamp()+$3::bigint * interval '1 microsecond' ELSE available_at END WHERE id=$1`, o.JobID, state, delay.Microseconds())
	return err
}

func (q *Queue) ReapExpired(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		return 0, ErrInvalid
	}
	type expired struct {
		o      Ownership
		n, max int32
	}
	count, retries := 0, 0
	err := pgx.BeginFunc(ctx, q.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text,lease_owner,lock_token::text,attempt_count,max_attempts FROM jobs
 WHERE state='running' AND lease_expires_at<=statement_timestamp() ORDER BY lease_expires_at,id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return err
		}
		batch, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (expired, error) {
			var e expired
			err := row.Scan(&e.o.JobID, &e.o.Worker, &e.o.Token, &e.n, &e.max)
			return e, err
		})
		if err != nil {
			return err
		}
		for _, e := range batch {
			if e.n < e.max {
				retries++
			}
			if err := finish(ctx, tx, e.o, e.n, e.max, "expired", nil); err != nil {
				return err
			}
		}
		count = len(batch)
		return nil
	})
	if err != nil {
		return 0, err
	}
	q.Metrics.Recovered.Add(float64(count))
	if retries > 0 {
		q.Metrics.Retries.WithLabelValues("expired").Add(float64(retries))
	}
	return count, nil
}
