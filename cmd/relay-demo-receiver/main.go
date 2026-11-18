package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/amgazal/relay-queue/internal/metrics"
	"github.com/amgazal/relay-queue/internal/webhook"
	"github.com/jackc/pgx/v5/pgxpool"
)

func run(ctx context.Context, log *slog.Logger) error {
	url := os.Getenv("RELAY_DATABASE_URL")
	if url == "" {
		return errors.New("RELAY_DATABASE_URL required")
	}
	p, err := pgxpool.New(ctx, url)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	defer p.Close()
	m := metrics.New(nil)
	var fail bool
	if v := os.Getenv("RELAY_DEMO_FAIL_FIRST"); v != "" {
		fail, err = strconv.ParseBool(v)
		if err != nil {
			return errors.New("invalid RELAY_DEMO_FAIL_FIRST")
		}
	}
	handler, err := webhook.Receiver(p, os.Getenv("RELAY_DEMO_SIGNING_SECRET"), fail, m)
	if err != nil {
		return err
	}
	stopMetrics, err := m.Listen(os.Getenv("RELAY_RECEIVER_METRICS_ADDR"), log)
	if err != nil {
		return err
	}
	defer stopMetrics()
	addr := os.Getenv("RELAY_RECEIVER_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8082"
	}
	cert, key := os.Getenv("RELAY_RECEIVER_TLS_CERT"), os.Getenv("RELAY_RECEIVER_TLS_KEY")
	if (cert == "") != (key == "") {
		return errors.New("configure both receiver TLS files")
	}
	srv := http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384}
	done := make(chan error, 1)
	go func() {
		if cert != "" {
			done <- srv.ListenAndServeTLS(cert, key)
		} else {
			done <- srv.ListenAndServe()
		}
	}()
	log.Info("demo receiver starting", "address", srv.Addr)
	select {
	case <-done:
		return errors.New("demo listener stopped")
	case <-ctx.Done():
	}
	drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(drain)
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("component", "demo_receiver")
	if err := run(ctx, log); err != nil {
		log.Error("receiver stopped", "error", err)
		os.Exit(1)
	}
}
