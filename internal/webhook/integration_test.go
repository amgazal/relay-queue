package webhook

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amgazal/relay-queue/internal/metrics"
	"github.com/amgazal/relay-queue/internal/queue"
	"github.com/amgazal/relay-queue/internal/testdb"
	"github.com/amgazal/relay-queue/internal/worker"
)

func require(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func TestReceiver(t *testing.T) {
	pool := testdb.Open(t)
	h, err := Receiver(pool, secret, true, metrics.New(nil))
	require(t, err)
	body := `{"event_id":"` + eventID + `","event_type":"sample","data":null}`
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	send := func(stamp, raw, signed, key string, want int) {
		t.Helper()
		r := httptest.NewRequest("POST", "/events", strings.NewReader(raw))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Relay-Timestamp", stamp)
		r.Header.Set("X-Relay-Event-ID", eventID)
		r.Header.Set("X-Relay-Signature", "sha256="+hex.EncodeToString(mac(key, stamp, eventID, []byte(signed))))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatal(w.Code, want)
		}
	}
	send(stamp, body, body, "wrong", 401)
	send("1", body, body, secret, 401)
	send(stamp, body+" ", body, secret, 401)
	send(stamp, "{}", "{}", secret, 400)
	send(stamp, body, body, secret, 503)
	send(stamp, body, body, secret, 204)
	h, err = Receiver(pool, secret, false, metrics.New(nil))
	require(t, err)
	send(stamp, body, body, secret, 204)
	var n int
	require(t, pool.QueryRow(t.Context(), "SELECT count(*) FROM demo_received_events").Scan(&n))
	if n != 1 {
		t.Fatal(n)
	}
}
func runWorker(t *testing.T, q *queue.Queue, id string, h worker.Handler) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	w, e := worker.New(q, worker.Config{ID: id, Concurrency: 1, Lease: 5 * time.Second, Timeout: time.Minute, Poll: 10 * time.Millisecond, Reap: 30 * time.Millisecond, Grace: 100 * time.Millisecond}, worker.Registry{Kind: h}, slog.New(slog.DiscardHandler))
	require(t, e)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	t.Cleanup(func() { cancel(); wait(t, done) })
	return cancel, done
}
func wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for worker or receiver")
	}
}
func TestCrashAfterAcceptance(t *testing.T) {
	p := testdb.Open(t)
	q := queue.New(p)
	h, e := Receiver(p, secret, false, metrics.New(nil))
	require(t, e)
	accepted, disconnected := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Commit the real effect first. Withhold its acknowledgement from the first worker.
		ack := httptest.NewRecorder()
		h.ServeHTTP(ack, r)
		if ack.Code != 204 {
			t.Errorf("receiver returned %d", ack.Code)
			w.WriteHeader(ack.Code)
			return
		}
		if calls.Add(1) == 1 {
			close(accepted)
			select {
			case <-r.Context().Done():
			case <-time.After(10 * time.Second):
			}
			close(disconnected)
			return
		}
		w.WriteHeader(ack.Code)
	}))
	defer srv.Close()
	s := sender(t, srv.URL+"/events")
	j, e := q.Enqueue(t.Context(), queue.Submission{Kind: Kind, Payload: []byte(job), MaxAttempts: 2})
	require(t, e)
	stop, done := runWorker(t, q, "original", s.Deliver)
	wait(t, accepted)
	stop()
	wait(t, done)
	wait(t, disconnected)
	var old queue.Ownership
	old.JobID = j.ID
	require(t, p.QueryRow(t.Context(), "SELECT lease_owner,lock_token::text FROM jobs WHERE id=$1 AND state='running'", j.ID).Scan(&old.Worker, &old.Token))
	_, e = p.Exec(t.Context(), "UPDATE jobs SET lease_expires_at=now()-interval '1 minute' WHERE id=$1", j.ID)
	require(t, e)
	if e = q.Complete(t.Context(), old); !errors.Is(e, queue.ErrLeaseLost) {
		t.Fatal(e)
	}
	runWorker(t, q, "replacement", s.Deliver)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		d, e := q.Get(t.Context(), j.ID)
		require(t, e)
		if d.Status == "succeeded" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	var history []string
	var effects int
	require(t, p.QueryRow(t.Context(), "SELECT array_agg(outcome ORDER BY attempt_number) FROM job_attempts WHERE job_id=$1", j.ID).Scan(&history))
	require(t, p.QueryRow(t.Context(), "SELECT count(*) FROM demo_received_events WHERE event_id=$1", eventID).Scan(&effects))
	if strings.Join(history, ",") != "expired,succeeded" || calls.Load() != 2 || effects != 1 {
		t.Fatal(history, calls.Load(), effects)
	}
}
func TestWorkerHTTPFailurePolicy(t *testing.T) {
	p := testdb.Open(t)
	q := queue.New(p)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(429)
		} else {
			w.WriteHeader(422)
		}
	}))
	defer srv.Close()
	s := sender(t, srv.URL)
	j, e := q.Enqueue(t.Context(), queue.Submission{Kind: Kind, Payload: json.RawMessage(job), MaxAttempts: 5})
	require(t, e)
	runWorker(t, q, "worker", s.Deliver)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		d, e := q.Get(t.Context(), j.ID)
		require(t, e)
		if d.Status == "dead" && d.Attempts == 2 && calls.Load() == 2 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("permanent failure did not terminate retry")
}
