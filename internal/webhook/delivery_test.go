package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amgazal/relay-queue/internal/metrics"
	"github.com/amgazal/relay-queue/internal/worker"
)

const eventID = "550e8400-e29b-41d4-a716-446655440000"
const secret = "0123456789abcdef0123456789abcdef"
const job = `{"destination":"local-demo","event_id":"` + eventID + `","event_type":"ticket.resolved","data":{"ticket_id":"123"}}`

func sender(t *testing.T, url string) *Sender {
	t.Helper()
	cfg, err := json.Marshal(map[string]destination{"local-demo": {url, secret}})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSender(string(cfg), metrics.New(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}
func TestDelivery(t *testing.T) {
	for _, code := range []int{200, 204, 299, 408, 429, 500, 503, 301, 400, 401, 404} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				body, _ := io.ReadAll(r.Body)
				h := hmac.New(sha256.New, []byte(secret))
				fmt.Fprintf(h, "%s.%s.", r.Header.Get("X-Relay-Timestamp"), eventID)
				h.Write(body)
				if r.Method != "POST" || r.Header.Get("X-Relay-Event-ID") != eventID || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-Relay-Signature") != "sha256="+hex.EncodeToString(h.Sum(nil)) || bytes.Contains(body, []byte("destination")) {
					t.Error("invalid signed request")
				}
				w.Header().Set("Location", "/redirected")
				w.WriteHeader(code)
			}))
			defer srv.Close()
			s := sender(t, srv.URL)
			err := s.Deliver(t.Context(), []byte(job))
			permanent := code == 301 || code == 400 || code == 401 || code == 404
			if (err == nil) != (code < 300) || errors.Is(err, worker.ErrPermanent) != permanent || requests.Load() != 1 {
				t.Fatal(err, requests.Load())
			}
		})
	}
}
func TestDestinationAndTransport(t *testing.T) {
	for _, url := range []string{"http://example.com", "file:///tmp/webhook", "https://user@example.com", "https://example.com/?secret=bad"} {
		cfg, _ := json.Marshal(map[string]destination{"x": {url, secret}})
		if _, err := NewSender(string(cfg), metrics.New(nil)); err == nil {
			t.Fatal(url)
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
	s := sender(t, srv.URL)
	for _, bad := range []string{strings.Replace(job, "local-demo", srv.URL, 1), strings.Replace(job, "local-demo", "missing", 1), strings.Replace(job, "event_type", "url", 1)} {
		if err := s.Deliver(t.Context(), []byte(bad)); !errors.Is(err, worker.ErrPermanent) {
			t.Fatal(err)
		}
	}
	s.client.Timeout = 100 * time.Millisecond
	if err := s.Deliver(t.Context(), []byte(job)); err == nil || errors.Is(err, worker.ErrPermanent) {
		t.Fatal(err)
	}
	srv.Close()
	if err := s.Deliver(t.Context(), []byte(job)); err == nil || errors.Is(err, worker.ErrPermanent) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.Deliver(ctx, []byte(job)); err == nil {
		t.Fatal("ignored cancellation")
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestBoundedRead(t *testing.T) {
	s := sender(t, "http://127.0.0.1:1")
	reader := strings.NewReader(strings.Repeat("x", responseLimit*2))
	s.client.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(reader)}, nil
	})
	if err := s.Deliver(t.Context(), []byte(job)); err != nil || reader.Len() != responseLimit {
		t.Fatal(err, reader.Len())
	}
}
