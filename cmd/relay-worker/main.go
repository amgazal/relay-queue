package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/amgazal/relay-queue/internal/metrics"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/amgazal/relay-queue/internal/queue"
	"github.com/amgazal/relay-queue/internal/webhook"
	"github.com/amgazal/relay-queue/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func config() (worker.Config, error) {
	c := worker.Config{ID: os.Getenv("RELAY_WORKER_ID"), Concurrency: 4, Lease: 30 * time.Second, Poll: 250 * time.Millisecond, Reap: 5 * time.Second, Timeout: time.Minute, Grace: 20 * time.Second}
	if c.ID == "" {
		c.ID = rand.Text()
	}
	if v, ok := os.LookupEnv("RELAY_CONCURRENCY"); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return c, errors.New("RELAY_CONCURRENCY must be a positive integer")
		}
		c.Concurrency = n
	}
	for name, target := range map[string]*time.Duration{"RELAY_LEASE": &c.Lease, "RELAY_POLL_INTERVAL": &c.Poll, "RELAY_REAP_INTERVAL": &c.Reap, "RELAY_HANDLER_TIMEOUT": &c.Timeout, "RELAY_SHUTDOWN_GRACE": &c.Grace} {
		if v, ok := os.LookupEnv(name); ok {
			d, err := time.ParseDuration(v)
			if err != nil || d <= 0 {
				return c, fmt.Errorf("%s requires a positive duration", name)
			}
			*target = d
		}
	}
	return c, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
func demos() worker.Registry {
	return worker.Registry{
		"demo.noop":  func(context.Context, json.RawMessage) error { return nil },
		"demo.sleep": func(ctx context.Context, _ json.RawMessage) error { return sleep(ctx, 50*time.Millisecond) },
		"demo.fail":  func(context.Context, json.RawMessage) error { return errors.New("intentional demonstration failure") },
		"demo.wait": func(ctx context.Context, p json.RawMessage) error {
			var in struct{ Duration string }
			if err := json.Unmarshal(p, &in); err != nil {
				return err
			}
			d, err := time.ParseDuration(in.Duration)
			if err != nil || d < 0 {
				return errors.New("duration must be a nonnegative Go duration")
			}
			return sleep(ctx, d)
		},
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	cfg, err := config()
	if err != nil {
		return err
	}
	url := os.Getenv("RELAY_DATABASE_URL")
	if url == "" {
		return errors.New("set RELAY_DATABASE_URL to the PostgreSQL connection string")
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return errors.New("invalid RELAY_DATABASE_URL")
	}
	defer pool.Close()
	m := metrics.New(nil)
	sender, err := webhook.NewSender(os.Getenv("RELAY_WEBHOOK_DESTINATIONS"), m)
	if err != nil {
		return err
	}
	defer sender.Close()
	handlers := demos()
	handlers[webhook.Kind] = sender.Deliver
	w, err := worker.New(queue.WithMetrics(pool, m), cfg, handlers, log)
	if err != nil {
		return err
	}
	if err = pool.Ping(ctx); err != nil {
		return errors.New("PostgreSQL connection failed")
	}
	closeMetrics, err := m.Listen(os.Getenv("RELAY_WORKER_METRICS_ADDR"), log)
	if err != nil {
		return err
	}
	defer closeMetrics()
	w.Run(ctx)
	return nil
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("component", "worker")
	if err := run(ctx, log); err != nil && ctx.Err() == nil {
		log.Error("worker startup failed", "error", err)
		os.Exit(1)
	}
}
