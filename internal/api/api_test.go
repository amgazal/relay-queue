package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amgazal/relay-queue/internal/queue"
	"github.com/amgazal/relay-queue/internal/testdb"
	"github.com/amgazal/relay-queue/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fixture struct {
	*testing.T
	p *pgxpool.Pool
	s *httptest.Server
}

const credential = "test-credential"

func setup(t *testing.T, limit int) *fixture {
	p := testdb.Open(t)
	h, e := New(p, credential, limit, slog.New(slog.DiscardHandler))
	if e != nil {
		t.Fatal(e)
	}
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return &fixture{t, p, s}
}

type reply struct {
	status int
	data   map[string]any
}

func (f *fixture) request(method, path, body, key string) (reply, error) {
	req, e := http.NewRequestWithContext(f.T.Context(), method, f.s.URL+path, strings.NewReader(body))
	if e != nil {
		return reply{}, e
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	res, e := f.s.Client().Do(req)
	if e != nil {
		return reply{}, e
	}
	defer res.Body.Close()
	var r reply
	r.status = res.StatusCode
	e = json.NewDecoder(res.Body).Decode(&r.data)
	if res.Header.Get("X-Request-ID") == "" {
		return r, errors.New("missing request ID")
	}
	if e == nil && r.status >= 400 {
		v, ok := r.data["error"].(map[string]any)
		if !ok || v["code"] == nil || v["message"] == nil {
			return r, errors.New("invalid error envelope")
		}
	}
	return r, e
}
func (f *fixture) call(method, path, body string, status int) map[string]any {
	f.Helper()
	r, e := f.request(method, path, body, credential)
	if e != nil || r.status != status {
		f.Fatalf("%s %s: %v %+v", method, path, e, r)
	}
	return r.data
}
func TestValidation(t *testing.T) {
	for _, s := range []string{`{}`, `{"type":"x"}`, `{"type":"","payload":1}`, `{"type":"x","payload":1,"new":1}`, `{"type":"x","payload":1,"max_attempts":0}`, `{"type":"x","type":"y","payload":1}`, `{"type":"x","payload":null}{}`} {
		if _, ok := decode([]byte(s)); ok {
			t.Fatal(s)
		}
	}
}
func TestHTTP(t *testing.T) {
	f := setup(t, 0)
	for _, key := range []string{"", "wrong"} {
		r, e := f.request("GET", "/v1/jobs", "", key)
		if e != nil || r.status != 401 {
			t.Fatal(r, e)
		}
	}
	f.call("GET", "/readyz", "", 200)
	f.call("POST", "/v1/jobs", "{", 400)
	f.call("POST", "/v1/jobs", strings.Repeat(" ", bodyLimit+1), 413)
	f.call("GET", "/v1/jobs/nope", "", 400)
	f.call("GET", "/v1/jobs/00000000-0000-0000-0000-000000000001", "", 404)
	for _, query := range []string{"limit=0", "limit=101", "cursor=!", "status=bad", "limit=1&limit=2"} {
		f.call("GET", "/v1/jobs?"+query, "", 400)
	}
	stats := f.call("GET", "/v1/stats", "", 200)
	if stats["oldest_queued_age_seconds"] != nil {
		t.Fatal(stats)
	}
	ids := map[string]bool{}
	for range 4 {
		id := f.call("POST", "/v1/jobs", `{"type":"echo","payload":null}`, 201)["id"].(string)
		ids[id] = true
	}
	f.call("POST", "/v1/jobs", `{"type":"other","payload":{}}`, 201)
	_, e := f.p.Exec(t.Context(), "UPDATE jobs SET created_at='2026-01-01'")
	if e != nil {
		t.Fatal(e)
	}
	path := "/v1/jobs?status=queued&type=echo&limit=2"
	for range 2 {
		data := f.call("GET", path, "", 200)
		rows := data["jobs"].([]any)
		if len(rows) != 2 {
			t.Fatal(data)
		}
		for _, row := range rows {
			id := row.(map[string]any)["id"].(string)
			if !ids[id] {
				t.Fatal(id)
			}
			delete(ids, id)
		}
		if cursor, ok := data["next_cursor"].(string); ok {
			path = "/v1/jobs?status=queued&type=echo&limit=2&cursor=" + cursor
		}
	}
	if len(ids) != 0 {
		t.Fatal(ids)
	}
	f.p.Close()
	f.call("GET", "/readyz", "", 503)
	r, e := f.request("GET", "/healthz", "", "")
	if e != nil || r.status != 200 {
		t.Fatal(e, r)
	}
}
func TestAdmissionAndReplay(t *testing.T) {
	f := setup(t, 2)
	body := `{"type":"delay","payload":1,"delay_seconds":10,"idempotency_key":"one"}`
	concurrent := func(same bool) map[int]int {
		ch := make(chan reply, 20)
		var wg sync.WaitGroup
		for i := range 20 {
			wg.Go(func() {
				b := body
				if !same {
					b = fmt.Sprintf(`{"type":"extra","payload":%d}`, i)
				}
				r, e := f.request("POST", "/v1/jobs", b, credential)
				if e != nil {
					t.Error(e)
				}
				ch <- r
			})
		}
		wg.Wait()
		close(ch)
		counts := map[int]int{}
		for r := range ch {
			counts[r.status]++
		}
		return counts
	}
	if c := concurrent(true); c[201] != 1 || c[200] != 19 {
		t.Fatal(c)
	}
	if c := concurrent(false); c[201] != 1 || c[429] != 19 {
		t.Fatal(c)
	}
	id := f.call("POST", "/v1/jobs", body, 200)["id"].(string)
	f.call("POST", "/v1/jobs", strings.Replace(body, "10", "11", 1), 409)
	path := "/v1/jobs/" + id
	f.call("POST", path+"/retry", "", 409)
	f.call("POST", path+"/cancel", "", 200)
	f.call("POST", path+"/cancel", "", 409)
	if f.call("GET", path, "", 200)["status"] != "cancelled" {
		t.Fatal(id)
	}
}

func TestHTTPWithTwoWorkers(t *testing.T) {
	f := setup(t, 0)
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	defer closeIfOpen(release)
	h := worker.Registry{"ok": func(ctx context.Context, _ json.RawMessage) error {
		entered <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, "fail": func(context.Context, json.RawMessage) error { return errors.New("demo failure") }}
	for i := range 2 {
		p, e := pgxpool.NewWithConfig(ctx, f.p.Config())
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(p.Close)
		w, e := worker.New(queue.New(p), worker.Config{ID: fmt.Sprint(i), Concurrency: 1, Lease: 10 * time.Second, Poll: 10 * time.Millisecond, Reap: time.Second, Timeout: time.Minute, Grace: time.Second}, h, slog.New(slog.DiscardHandler))
		if e != nil {
			t.Fatal(e)
		}
		wg.Go(func() { w.Run(ctx) })
	}
	var ids []string
	for range 2 {
		ids = append(ids, f.call("POST", "/v1/jobs", `{"type":"ok","payload":{}}`, 201)["id"].(string))
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("workers did not start")
		}
	}
	f.call("POST", "/v1/jobs/"+ids[0]+"/cancel", "", 409)
	close(release)
	body := `{"type":"fail","payload":1,"max_attempts":1,"idempotency_key":"test"}`
	fail := f.call("POST", "/v1/jobs", body, 201)["id"].(string)
	wait := func(id, status string, attempt int) {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			r := f.call("GET", "/v1/jobs/"+id, "", 200)
			if r["status"] == status && r["attempt_count"] == float64(attempt) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal(id, status)
	}
	for _, id := range ids {
		wait(id, "succeeded", 1)
	}
	wait(fail, "dead", 1)
	f.call("POST", "/v1/jobs/"+fail+"/retry", "", 200)
	wait(fail, "dead", 2)
	f.call("POST", "/v1/jobs", body, 200)
	data := f.call("GET", "/v1/stats", "", 200)
	if data["dead"] != float64(1) || data["completions_last_60s"] != float64(2) {
		t.Fatal(data)
	}
	var good bool
	e := f.p.QueryRow(ctx, "SELECT count(*)=4 AND count(DISTINCT worker_id)=2 AND count(*) FILTER(WHERE outcome='failed')=2 FROM job_attempts").Scan(&good)
	if e != nil || !good {
		t.Fatal(e, good)
	}
}
func closeIfOpen(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func TestScheduleEndpoints(t *testing.T) {
	f := setup(t, 0)
	path := "/v1/schedules"
	body := `{"name":"minute","type":"demo.sleep","payload":null,"cron":"* * * * *"}`
	r, err := f.request("POST", path, body, "")
	if err != nil || r.status != 401 {
		t.Fatal(r, err)
	}
	for _, invalid := range []string{strings.Replace(body, "* * * * *", "@daily", 1), strings.Replace(body, "demo.sleep", " ", 1), strings.Replace(body, "payload", "extra", 1)} {
		f.call("POST", path, invalid, 400)
	}
	id := f.call("POST", path, body, 201)["id"].(string)
	f.call("POST", path, body, 201)
	page := f.call("GET", path+"?limit=1", "", 200)
	last := f.call("GET", path+"?limit=1&cursor="+page["next_cursor"].(string), "", 200)
	if last["next_cursor"] != nil || len(last["schedules"].([]any)) != 1 {
		t.Fatal(last)
	}
	f.call("GET", path+"/"+id, "", 200)
	for range 2 {
		data := f.call("DELETE", path+"/"+id, "", 200)
		if data["enabled"] != false {
			t.Fatal(data)
		}
	}
	f.call("GET", path+"/00000000-0000-0000-0000-000000000000", "", 404)
}
