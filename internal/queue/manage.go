package queue

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	ErrMissing    Error = "job not found"
	ErrTransition Error = "invalid state transition"
	ErrCapacity   Error = "active job limit reached"
)

// Serialize admissions across API replicas. Worker transitions never increase active depth.
func admissionLock(ctx context.Context, tx pgx.Tx, limit int) error {
	if limit == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(78235620)")
	return err
}
func checkCapacity(ctx context.Context, tx pgx.Tx, limit int) error {
	if limit == 0 {
		return nil
	}
	var exceeded bool
	err := tx.QueryRow(ctx, "SELECT count(*)>$1 FROM jobs WHERE state IN ('queued','running')", limit).Scan(&exceeded)
	if err == nil && exceeded {
		return ErrCapacity
	}
	return err
}

type Detail struct {
	ID        string     `json:"id"`
	Kind      string     `json:"type"`
	Status    string     `json:"status"`
	Priority  int32      `json:"priority"`
	Attempts  int32      `json:"attempt_count"`
	Max       int32      `json:"max_attempts"`
	Created   time.Time  `json:"created_at"`
	Scheduled time.Time  `json:"scheduled_at"`
	Completed *time.Time `json:"completed_at,omitempty"`
	LastError *string    `json:"last_error,omitempty"`
}

const detailFields = `j.id::text,j.kind,j.state,j.priority,j.attempt_count,j.max_attempts,j.created_at,j.available_at,j.completed_at,
 (SELECT error_message FROM job_attempts WHERE job_id=j.id AND error_message IS NOT NULL ORDER BY attempt_number DESC LIMIT 1)`

func scanDetail(row pgx.Row) (Detail, error) {
	var d Detail
	err := row.Scan(&d.ID, &d.Kind, &d.Status, &d.Priority, &d.Attempts, &d.Max, &d.Created, &d.Scheduled, &d.Completed, &d.LastError)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrMissing
	}
	return d, err
}
func (q *Queue) Get(ctx context.Context, id string) (Detail, error) {
	return scanDetail(q.pool.QueryRow(ctx, "SELECT "+detailFields+" FROM jobs j WHERE id=$1", id))
}

type Cursor struct {
	Created time.Time
	ID      string
}

func (q *Queue) List(ctx context.Context, status, kind string, after *Cursor, limit int) ([]Detail, error) {
	var created *time.Time
	var id *string
	if after != nil {
		created = &after.Created
		id = &after.ID
	}
	rows, err := q.pool.Query(ctx, `SELECT `+detailFields+` FROM jobs j WHERE ($1='' OR state=$1) AND ($2='' OR kind=$2)
 AND ($3::timestamptz IS NULL OR (created_at,id)<($3,$4::uuid)) ORDER BY created_at DESC,id DESC LIMIT $5`, status, kind, created, id, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Detail, error) { return scanDetail(r) })
}
func (q *Queue) Cancel(ctx context.Context, id string) (Detail, error) {
	return q.transition(ctx, id, false, 0)
}
func (q *Queue) Retry(ctx context.Context, id string, limit int) (Detail, error) {
	d, err := q.transition(ctx, id, true, limit)
	if err == nil {
		q.Metrics.Retries.WithLabelValues("manual").Inc()
	}
	return d, err
}
func (q *Queue) transition(ctx context.Context, id string, retry bool, limit int) (Detail, error) {
	var result Detail
	err := pgx.BeginTxFunc(ctx, q.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		if err := admissionLock(ctx, tx, limit); err != nil {
			return err
		}
		var state string
		var max int32
		err := tx.QueryRow(ctx, "SELECT state,max_attempts FROM jobs WHERE id=$1 FOR UPDATE", id).Scan(&state, &max)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrMissing
		}
		if err != nil {
			return err
		}
		sql := "UPDATE jobs AS j SET state='cancelled',updated_at=now() WHERE id=$1"
		if retry {
			if state != "dead" || max == 2147483647 {
				return ErrTransition
			}
			sql = "UPDATE jobs AS j SET state='queued',original_max_attempts=coalesce(original_max_attempts,max_attempts),max_attempts=max_attempts+1,available_at=now(),updated_at=now() WHERE id=$1"
		} else if state != "queued" {
			return ErrTransition
		}
		result, err = scanDetail(tx.QueryRow(ctx, sql+" RETURNING "+detailFields, id))
		if err != nil {
			return err
		}
		return checkCapacity(ctx, tx, limit)
	})
	return result, err
}

type Stats struct {
	Counts      map[string]int64 `json:"counts"`
	Queued      int64            `json:"queue_depth"`
	Running     int64            `json:"running"`
	Dead        int64            `json:"dead"`
	Oldest      *float64         `json:"oldest_queued_age_seconds"`
	Completions int64            `json:"completions_last_60s"`
}

func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	var succeeded, cancelled int64
	err := q.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE state='queued'),count(*) FILTER(WHERE state='running'),count(*) FILTER(WHERE state='dead'),count(*) FILTER(WHERE state='succeeded'),count(*) FILTER(WHERE state='cancelled'),
 extract(epoch FROM now()-min(created_at) FILTER(WHERE state='queued'))::float8,count(*) FILTER(WHERE completed_at>=now()-interval '60 seconds' AND state='succeeded') FROM jobs`).Scan(&s.Queued, &s.Running, &s.Dead, &succeeded, &cancelled, &s.Oldest, &s.Completions)
	s.Counts = map[string]int64{"queued": s.Queued, "running": s.Running, "dead": s.Dead, "succeeded": succeeded, "cancelled": cancelled}
	return s, err
}
