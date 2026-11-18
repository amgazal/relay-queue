package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (m *Metrics) Listen(addr string, log *slog.Logger) (func(), error) {
	if addr == "" {
		return func() {}, nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, errors.New("cannot bind metrics listener")
	}
	return m.serve(ln, log), nil
}
func (m *Metrics) serve(ln net.Listener, log *slog.Logger) func() {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2}))
	srv := http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics listener stopped", "error_category", "listener")
		}
	}()
	log.Info("metrics listening", "address", ln.Addr().String())
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if srv.Shutdown(ctx) != nil {
			_ = srv.Close()
		}
		<-done
	}
}
