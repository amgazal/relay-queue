// Package scheduler turns UTC cron occurrences into ordinary queue jobs.
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/amgazal/relay-queue/internal/metrics"
	"log/slog"
	"math/bits"
	"strings"
	"time"

	"github.com/amgazal/relay-queue/internal/queue"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	pool  *pgxpool.Pool
	limit int
	m     *metrics.Metrics
}

func New(p *pgxpool.Pool, limit int) *Scheduler { return WithMetrics(p, limit, metrics.New(nil)) }

func WithMetrics(p *pgxpool.Pool, limit int, m *metrics.Metrics) *Scheduler {
	return &Scheduler{p, limit, m}
}

type Input struct {
	Name    string          `json:"name"`
	Cron    string          `json:"cron"`
	Kind    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}
type Schedule struct {
	Input
	ID      string    `json:"id"`
	Enabled bool      `json:"enabled"`
	Next    time.Time `json:"next_run"`
	Created time.Time `json:"created_at"`
	Updated time.Time `json:"updated_at"`
}

var ErrInvalid = errors.New("invalid recurring schedule")
var ErrMissing = errors.New("schedule not found")
var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

func next(expr string, after time.Time) (time.Time, error) {
	if len(strings.Fields(expr)) != 5 || strings.ContainsAny(expr, "=@?") || len(expr) > 256 {
		return time.Time{}, ErrInvalid
	}
	parsed, err := parser.Parse(expr)
	if err != nil {
		return time.Time{}, ErrInvalid
	}
	at := parsed.Next(after.UTC())
	if at.IsZero() {
		return time.Time{}, ErrInvalid
	}
	return at, nil
}

const fields = "name,cron,kind,payload,id::text,enabled,next_run,created_at,updated_at"

func scan(row pgx.Row) (Schedule, error) {
	var s Schedule
	err := row.Scan(&s.Name, &s.Cron, &s.Kind, &s.Payload, &s.ID, &s.Enabled, &s.Next, &s.Created, &s.Updated)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrMissing
	}
	return s, err
}
func (s *Scheduler) Create(ctx context.Context, in Input) (Schedule, error) {
	if strings.TrimSpace(in.Name) == "" || len(in.Name) > 128 || strings.TrimSpace(in.Kind) == "" || len(in.Kind) > 128 || !json.Valid(in.Payload) {
		return Schedule{}, ErrInvalid
	}
	var result Schedule
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var now time.Time
		if err := tx.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
			return err
		}
		at, err := next(in.Cron, now)
		if err != nil {
			return err
		}
		result, err = scan(tx.QueryRow(ctx, "INSERT INTO schedules(name,cron,kind,payload,next_run) VALUES($1,$2,$3,$4,$5) RETURNING "+fields, in.Name, in.Cron, in.Kind, in.Payload, at))
		return err
	})
	return result, err
}
func (s *Scheduler) Get(ctx context.Context, id string) (Schedule, error) {
	return scan(s.pool.QueryRow(ctx, "SELECT "+fields+" FROM schedules WHERE id=$1", id))
}
func (s *Scheduler) Disable(ctx context.Context, id string) (Schedule, error) {
	return scan(s.pool.QueryRow(ctx, "UPDATE schedules SET enabled=false,updated_at=CASE WHEN enabled THEN clock_timestamp() ELSE updated_at END WHERE id=$1 RETURNING "+fields, id))
}
func (s *Scheduler) List(ctx context.Context, c *queue.Cursor, limit int) ([]Schedule, error) {
	var at *time.Time
	var id *string
	if c != nil {
		at = &c.Created
		id = &c.ID
	}
	rows, err := s.pool.Query(ctx, "SELECT "+fields+" FROM schedules WHERE ($1::timestamptz IS NULL OR (created_at,id)<($1,$2::uuid)) ORDER BY created_at DESC,id DESC LIMIT $3", at, id, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Schedule, error) { return scan(r) })
}

// Dispatch handles at most one schedule so capacity and rollback are independent per schedule.
func (s *Scheduler) Dispatch(ctx context.Context) (bool, error) {
	start := time.Now()
	dispatched, created := false, false
	var expression string
	var occurrence, cutoff time.Time
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		var id, expr, kind string
		var payload json.RawMessage
		var due, now time.Time
		err := tx.QueryRow(ctx, `SELECT id::text,cron,kind,payload,next_run,statement_timestamp() FROM schedules
 WHERE enabled AND next_run<=statement_timestamp() ORDER BY next_run,id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&id, &expr, &kind, &payload, &due, &now)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		at, err := next(expr, now)
		if err != nil {
			return err
		}
		var exists bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT FROM jobs WHERE schedule_id=$1 AND occurrence_at=$2)", id, due).Scan(&exists); err != nil {
			return err
		}
		expression, occurrence, cutoff = expr, due, now
		if !exists {
			created = true
			j, _, err := queue.SubmitTx(ctx, tx, queue.Submission{Kind: kind, Payload: payload, ScheduledAt: &due}, 0, s.limit)
			if err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, "UPDATE jobs SET schedule_id=$1,occurrence_at=$2 WHERE id=$3", id, due, j.ID); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, "UPDATE schedules SET next_run=$1,updated_at=clock_timestamp() WHERE id=$2", at, id)
		dispatched = err == nil
		return err
	})
	s.m.DispatchTime.Observe(time.Since(start).Seconds())
	if err != nil && ctx.Err() == nil {
		s.m.DispatchFailures.Inc()
	}
	if err == nil && dispatched {
		if created {
			s.m.Submitted.Inc()
			s.m.Dispatched.Inc()
		}
		s.m.Skipped.Add(float64(skipped(expression, occurrence, cutoff)))
	}
	return dispatched && err == nil, err
}
func (s *Scheduler) Run(ctx context.Context, interval time.Duration, log *slog.Logger) {
	for ctx.Err() == nil {
		for range 64 {
			ok, err := s.Dispatch(ctx)
			if err != nil && ctx.Err() == nil {
				log.Warn("schedule dispatch deferred", "error_category", "dispatch")
			}
			if err != nil || !ok {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// Count intervening UTC matches after commit. On fully covered dates, all
// allowed hour/minute combinations occur once, even after a long outage.
func skipped(expr string, due, cutoff time.Time) int64 {
	if cutoff.Sub(due) < time.Minute {
		return 0
	}
	schedule, err := parser.Parse(expr)
	if err != nil {
		return 0
	}
	spec := schedule.(*cron.SpecSchedule)
	var n int64
	for at := spec.Next(due.UTC()); !at.IsZero() && !at.After(cutoff); {
		day := at.Truncate(24 * time.Hour)
		end := day.Add(24 * time.Hour)
		if day.After(due) && !end.After(cutoff) {
			n += int64(bits.OnesCount64(spec.Hour&((1<<24)-1)) * bits.OnesCount64(spec.Minute&((1<<60)-1)))
			at = spec.Next(end.Add(-time.Second))
		} else {
			n++
			at = spec.Next(at)
		}
	}
	return n
}
