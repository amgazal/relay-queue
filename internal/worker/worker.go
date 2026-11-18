// Package worker executes registered handlers against a durable Relay queue.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/amgazal/relay-queue/internal/queue"
)

type Handler func(context.Context, json.RawMessage) error
type Registry map[string]Handler

type Config struct {
	ID                                string
	Concurrency                       int
	Lease, Poll, Reap, Timeout, Grace time.Duration
}

type Worker struct {
	q        *queue.Queue
	cfg      Config
	handlers Registry
	log      *slog.Logger
}

func New(q *queue.Queue, c Config, handlers Registry, log *slog.Logger) (*Worker, error) {
	if q == nil || log == nil || strings.TrimSpace(c.ID) == "" || c.Concurrency < 1 || c.Lease < 3*time.Microsecond || c.Poll <= 0 || c.Reap <= 0 || c.Timeout <= 0 || c.Grace <= 0 {
		return nil, errors.New("worker requires queue, logger, ID, positive concurrency and durations, and lease >= 3us")
	}
	for kind, h := range handlers {
		if strings.TrimSpace(kind) == "" || h == nil {
			return nil, fmt.Errorf("invalid handler registration %q", kind)
		}
	}
	return &Worker{q, c, maps.Clone(handlers), log.With("worker", c.ID)}, nil
}

func (w *Worker) Run(ctx context.Context) {
	active, stop := context.WithCancel(context.WithoutCancel(ctx))
	defer stop()
	var wg sync.WaitGroup
	wg.Go(func() { w.recoverExpired(ctx) })
	for range w.cfg.Concurrency {
		wg.Go(func() { w.slot(ctx, active) })
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	w.log.Info("worker started", "concurrency", w.cfg.Concurrency)
	<-ctx.Done()
	w.log.Info("worker draining")
	select {
	case <-done:
		w.log.Info("worker drained")
	case <-time.After(w.cfg.Grace):
		stop()
		w.log.Warn("grace period expired")
	}
}

func (w *Worker) slot(poll, active context.Context) {
	for poll.Err() == nil {
		claims, err := w.q.Claim(poll, w.cfg.ID, 1, w.cfg.Lease)
		if poll.Err() != nil {
			return
		}
		if err == nil && len(claims) > 0 {
			w.execute(active, claims[0])
			continue
		}
		if err != nil {
			w.log.Error("claim failed", "error", err)
		}
		select {
		case <-poll.Done():
			return
		case <-time.After(w.cfg.Poll):
		}
	}
}

func (w *Worker) recoverExpired(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Reap)
	defer ticker.Stop()
	for ctx.Err() == nil {
		n, err := w.q.ReapExpired(ctx, w.cfg.Concurrency)
		if err != nil && ctx.Err() == nil {
			w.log.Error("recovery failed", "error", err)
		} else if n > 0 {
			w.log.Info("expired leases recovered", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func invoke(ctx context.Context, handlers Registry, job queue.Job) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("handler panic: %v", p)
		}
	}()
	h := handlers[job.Kind]
	if h == nil {
		return fmt.Errorf("unregistered job type %q", job.Kind)
	}
	return h(ctx, job.Payload)
}

func (w *Worker) execute(active context.Context, c queue.Claim) {
	log := w.log.With("job", c.Job.ID, "attempt", c.Job.AttemptCount, "kind", c.Job.Kind)
	// Reconfirm ownership before dispatch in case the claim response was delayed.
	renew := func(ctx context.Context) error {
		call, cancel := context.WithTimeout(ctx, w.cfg.Lease/3)
		defer cancel()
		_, err := w.q.Heartbeat(call, c.Ownership, w.cfg.Lease)
		return err
	}
	if err := renew(active); err != nil {
		log.Warn("cannot confirm lease", "error", err)
		return
	}
	run, cancel := context.WithTimeout(active, w.cfg.Timeout)
	defer cancel()
	finished := make(chan struct{})
	renewed := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(w.cfg.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-finished:
				renewed <- nil
				return
			case <-run.Done():
				renewed <- nil
				return
			case <-ticker.C:
				if err := renew(active); err != nil {
					cancel()
					renewed <- err
					return
				}
			}
		}
	}()
	log.Debug("handler started")
	err := invoke(run, w.handlers, c.Job)
	close(finished)
	if lost := <-renewed; lost != nil {
		log.Warn("lease renewal failed; abandoning attempt", "error", lost)
		return
	}
	if active.Err() != nil {
		return
	}
	if run.Err() != nil {
		err = run.Err()
	}
	final, stop := context.WithTimeout(active, w.cfg.Lease/3)
	defer stop()
	if err != nil {
		if dbErr := w.q.Fail(final, c.Ownership, err.Error()); dbErr != nil {
			log.Warn("failure could not be recorded", "error", dbErr)
			return
		}
		log.Warn("handler failed", "error", err)
		return
	}
	if err = w.q.Complete(final, c.Ownership); err != nil {
		log.Warn("completion could not be recorded", "error", err)
		return
	}
	log.Debug("job completed")
}
