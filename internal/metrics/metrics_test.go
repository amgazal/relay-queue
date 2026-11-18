package metrics

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func TestListenerAndIsolation(t *testing.T) {
	first, second := New(nil), New(nil)
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() { first.Attempts.Inc(); first.HandlerTime.Observe(.2) })
	}
	wg.Wait()
	for i, m := range []*Metrics{first, second} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		stop := m.serve(ln, slog.New(slog.DiscardHandler))
		defer stop()
		url := "http://" + ln.Addr().String()
		res, err := http.Get(url + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		want := "100"
		if i == 1 {
			want = "0"
		}
		for _, series := range []string{"relay_execution_attempts_total ", "relay_handler_duration_seconds_count "} {
			if !strings.Contains(string(b), series+want+"\n") {
				t.Fatal(series, want)
			}
		}
		res, err = http.Get(url + "/v1/jobs")
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 404 {
			t.Fatal(res.Status)
		}
		stop()
		if res, err = http.Get(url + "/metrics"); err == nil {
			res.Body.Close()
			t.Fatal("listener still open")
		}
	}
}
