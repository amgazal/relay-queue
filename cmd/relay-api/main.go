package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/amgazal/relay-queue/internal/metrics"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/amgazal/relay-queue/internal/api"
	"github.com/amgazal/relay-queue/internal/scheduler"
	"github.com/jackc/pgx/v5/pgxpool"
)

func run(ctx context.Context, log *slog.Logger) error {
	url := os.Getenv("RELAY_DATABASE_URL")
	if url == "" {
		return errors.New("RELAY_DATABASE_URL is required")
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return errors.New("invalid RELAY_DATABASE_URL")
	}
	defer pool.Close()
	limit := 0
	if raw, ok := os.LookupEnv("RELAY_MAX_ACTIVE_JOBS"); ok {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 0 {
			return errors.New("invalid RELAY_MAX_ACTIVE_JOBS")
		}
	}
	m := metrics.New(pool)
	handler, err := api.WithMetrics(pool, os.Getenv("RELAY_API_KEY"), limit, log.With("component", "api"), m)
	if err != nil {
		return fmt.Errorf("check RELAY_API_KEY: %w", err)
	}
	addr := os.Getenv("RELAY_API_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	srv := http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384}
	interval := time.Second
	if value, ok := os.LookupEnv("RELAY_SCHEDULER_INTERVAL"); ok {
		interval, err = time.ParseDuration(value)
		if err != nil || interval < time.Millisecond {
			return errors.New("RELAY_SCHEDULER_INTERVAL must be at least 1ms")
		}
	}
	metricsAddr, configured := os.LookupEnv("RELAY_API_METRICS_ADDR")
	if !configured {
		metricsAddr = "127.0.0.1:9091"
	}
	closeMetrics, err := m.Listen(metricsAddr, log)
	if err != nil {
		return err
	}
	defer closeMetrics()
	scheduling, stop := context.WithCancel(ctx)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		scheduler.WithMetrics(pool, limit, m).Run(scheduling, interval, log.With("component", "scheduler"))
	}()
	defer func() { stop(); <-stopped }()
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	drain, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(drain)
}
func main() {
	if len(os.Args) == 2 && os.Args[1] == "ready" {
		client := http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get("http://127.0.0.1:8080/readyz")
		if err != nil {
			os.Exit(1)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(ctx, log); err != nil {
		log.Error("API stopped", "component", "api", "error_category", "startup_or_listener")
		os.Exit(1)
	}
}
