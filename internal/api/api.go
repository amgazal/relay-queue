// Package api serves Relay's authenticated HTTP management interface.
package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/amgazal/relay-queue/internal/metrics"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/amgazal/relay-queue/internal/queue"
	"github.com/amgazal/relay-queue/internal/scheduler"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type server struct {
	q         *queue.Queue
	pool      *pgxpool.Pool
	limit     int
	schedules *scheduler.Scheduler
}

func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, status int, code, message string) {
	write(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func bad(w http.ResponseWriter, msg string) { problem(w, 400, "INVALID_REQUEST", msg) }
func dbError(w http.ResponseWriter, err error) {
	var pg *pgconn.PgError
	switch {
	case errors.Is(err, scheduler.ErrMissing):
		problem(w, 404, "NOT_FOUND", "Schedule does not exist.")
	case errors.Is(err, scheduler.ErrInvalid):
		bad(w, "Invalid recurring schedule.")
	case errors.Is(err, queue.ErrMissing):
		problem(w, 404, "NOT_FOUND", "Job does not exist.")
	case errors.Is(err, queue.ErrInvalid), errors.As(err, &pg) && strings.HasPrefix(pg.Code, "22"):
		bad(w, "Invalid job parameters.")
	case errors.Is(err, queue.ErrConflict):
		problem(w, 409, "IDEMPOTENCY_CONFLICT", "Key already identifies a different submission.")
	case errors.Is(err, queue.ErrTransition):
		problem(w, 409, "INVALID_TRANSITION", "Job cannot make this transition.")
	case errors.Is(err, queue.ErrCapacity):
		problem(w, 429, "QUEUE_FULL", "Admission limit reached.")
	default:
		problem(w, 503, "UNAVAILABLE", "Service temporarily unavailable.")
	}
}

type response struct {
	http.ResponseWriter
	status int
}

func (w *response) WriteHeader(code int) { w.status = code; w.ResponseWriter.WriteHeader(code) }

func New(pool *pgxpool.Pool, key string, limit int, log *slog.Logger) (http.Handler, error) {
	return WithMetrics(pool, key, limit, log, metrics.New(nil))
}
func WithMetrics(pool *pgxpool.Pool, key string, limit int, log *slog.Logger, m *metrics.Metrics) (http.Handler, error) {
	if pool == nil || log == nil || limit < 0 || strings.TrimSpace(key) == "" || strings.ContainsAny(key, " \r\n\t") {
		return nil, errors.New("invalid API configuration")
	}
	s := server{queue.WithMetrics(pool, m), pool, limit, scheduler.WithMetrics(pool, limit, m)}
	digest := sha256.Sum256([]byte(key))
	mux := http.NewServeMux()
	routes := make(map[string]bool)
	handle := func(pattern string, h http.HandlerFunc) { routes[pattern] = true; mux.HandleFunc(pattern, h) }
	handle("POST /v1/schedules", s.createSchedule)
	handle("GET /v1/schedules", s.listSchedules)
	handle("GET /v1/schedules/{id}", s.schedule)
	handle("DELETE /v1/schedules/{id}", s.schedule)
	handle("POST /v1/jobs", s.submit)
	handle("GET /v1/jobs", s.list)
	handle("GET /v1/jobs/{id}", s.job)
	handle("POST /v1/jobs/{id}/cancel", s.job)
	handle("POST /v1/jobs/{id}/retry", s.job)
	handle("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) {
		v, e := s.q.Stats(r.Context())
		if e != nil {
			dbError(w, e)
			return
		}
		write(w, 200, v)
	})
	handle("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { write(w, 200, map[string]string{"status": "alive"}) })
	handle("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if e := s.pool.Ping(ctx); e != nil {
			dbError(w, e)
			return
		}
		write(w, 200, map[string]string{"status": "ready"})
	})
	handle("/", func(w http.ResponseWriter, r *http.Request) {
		problem(w, 404, "NOT_FOUND", "Unknown endpoint or method.")
	})
	return http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
		id := rand.Text()
		w := &response{out, 200}
		w.Header().Set("X-Request-ID", id)
		w.Header().Set("Cache-Control", "no-store")
		start := time.Now()
		defer func() {
			if p := recover(); p != nil {
				problem(w, 500, "INTERNAL", "Internal server error.")
			}
			route := r.Pattern
			if !routes[route] {
				route = "unmatched"
			}
			m.Requests.WithLabelValues(route, strconv.Itoa(w.status)).Inc()
			m.RequestTime.WithLabelValues(route).Observe(time.Since(start).Seconds())
			level := slog.LevelDebug
			if w.status >= 500 {
				level = slog.LevelWarn
			}
			log.Log(r.Context(), level, "request", "request_id", id, "route", route, "status", w.status, "duration", time.Since(start))
		}()
		if r.URL.Path == "/v1" || strings.HasPrefix(r.URL.Path, "/v1/") {
			token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			got := sha256.Sum256([]byte(token))
			if !ok || len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare(got[:], digest[:]) != 1 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				problem(w, 401, "UNAUTHORIZED", "Bearer credential required.")
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		r = r.WithContext(ctx)
		mux.ServeHTTP(w, r)
	}), nil
}

const bodyLimit = 1 << 20

type submission struct {
	Type     string
	Payload  json.RawMessage
	Priority int32
	Max      int32
	Delay    int32
	Key      *string
}

func decode(data []byte) (submission, bool) {
	var s submission
	s.Max = 5
	fields := map[string]any{"type": &s.Type, "payload": &s.Payload, "priority": &s.Priority, "max_attempts": &s.Max, "delay_seconds": &s.Delay, "idempotency_key": &s.Key}
	if !decodeFields(data, fields) {
		return s, false
	}
	return s, strings.TrimSpace(s.Type) != "" && len(s.Type) <= 128 && len(s.Payload) > 0 && s.Priority >= -1000 && s.Priority <= 1000 && s.Max >= 1 && s.Max <= 1000 && s.Delay >= 0 && s.Delay <= 31536000 && (s.Key == nil || (strings.TrimSpace(*s.Key) != "" && len(*s.Key) <= 256))
}
func decodeFields(data []byte, fields map[string]any) bool {
	d := json.NewDecoder(bytes.NewReader(data))
	t, e := d.Token()
	if e != nil || t != json.Delim('{') || !utf8.Valid(data) {
		return false
	}

	for d.More() {
		k, e := d.Token()
		if e != nil {
			return false
		}
		name, ok := k.(string)
		target, exists := fields[name]
		if !ok || !exists {
			return false
		}
		delete(fields, name)
		var raw json.RawMessage
		if d.Decode(&raw) != nil || (name != "payload" && string(raw) == "null") || json.Unmarshal(raw, target) != nil {
			return false
		}
	}
	_, e = d.Token()
	if e != nil || d.Decode(new(any)) != io.EOF {
		return false
	}
	return true
}
func (s server) submit(w http.ResponseWriter, r *http.Request) {
	data, ok := readBody(w, r)
	if !ok {
		return
	}
	input, ok := decode(data)
	if !ok {
		bad(w, "Invalid job submission.")
		return
	}
	j, created, err := s.q.Submit(r.Context(), queue.Submission{Kind: input.Type, Payload: input.Payload, Priority: input.Priority, MaxAttempts: input.Max, IdempotencyKey: input.Key}, input.Delay, s.limit)
	if err != nil {
		dbError(w, err)
		return
	}
	code := 200
	if created {
		code = 201
	}
	w.Header().Set("Location", "/v1/jobs/"+j.ID)
	write(w, code, map[string]any{"id": j.ID, "type": j.Kind, "status": j.State, "priority": j.Priority, "attempt_count": j.AttemptCount, "max_attempts": j.MaxAttempts, "scheduled_at": j.AvailableAt})
}
func uuid(s string) bool {
	var u pgtype.UUID
	return len(s) == 36 && s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-' && u.Scan(s) == nil
}
func (s server) job(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuid(id) {
		bad(w, "Invalid job ID.")
		return
	}
	var j queue.Detail
	var err error
	switch r.Pattern {
	case "POST /v1/jobs/{id}/cancel":
		j, err = s.q.Cancel(r.Context(), id)
	case "POST /v1/jobs/{id}/retry":
		j, err = s.q.Retry(r.Context(), id, s.limit)
	default:
		j, err = s.q.Get(r.Context(), id)
	}
	if err != nil {
		dbError(w, err)
		return
	}
	write(w, 200, j)
}

func (s server) list(w http.ResponseWriter, r *http.Request) {
	v, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		bad(w, "Invalid query.")
		return
	}
	for k, vals := range v {
		if len(vals) != 1 || vals[0] == "" || (k != "limit" && k != "cursor" && k != "type" && k != "status") {
			bad(w, "Invalid query parameter.")
			return
		}
	}
	status, kind := v.Get("status"), v.Get("type")
	switch status {
	case "", "queued", "running", "succeeded", "dead", "cancelled":
	default:
		bad(w, "Invalid status.")
		return
	}
	if len(kind) > 128 || (kind != "" && strings.TrimSpace(kind) == "") {
		bad(w, "Invalid type.")
		return
	}
	limit, after, ok := page(w, v)
	if !ok {
		return
	}
	jobs, e := s.q.List(r.Context(), status, kind, after, limit+1)
	if e != nil {
		dbError(w, e)
		return
	}
	var next *string
	if len(jobs) > limit {
		jobs = jobs[:limit]
		last := jobs[limit-1]
		v := encodeCursor(last.Created, last.ID)
		next = &v
	}
	write(w, 200, map[string]any{"jobs": jobs, "next_cursor": next})
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, bodyLimit))
	if err != nil {
		var size *http.MaxBytesError
		if errors.As(err, &size) {
			problem(w, 413, "BODY_TOO_LARGE", "Request exceeds 1 MiB.")
		} else {
			bad(w, "Cannot read request.")
		}
		return nil, false
	}
	return data, true
}

func page(w http.ResponseWriter, v url.Values) (int, *queue.Cursor, bool) {
	var err error
	limit := 25
	if v.Has("limit") {
		limit, err = strconv.Atoi(v.Get("limit"))
		if err != nil || limit < 1 || limit > 100 {
			bad(w, "Limit must be 1 through 100.")
			return 0, nil, false
		}
	}
	var after *queue.Cursor
	if token := v.Get("cursor"); token != "" {
		data, e := base64.RawURLEncoding.DecodeString(token)
		stamp, id, found := strings.Cut(string(data), "|")
		at, te := time.Parse(time.RFC3339Nano, stamp)
		if e != nil || te != nil || !found || !uuid(id) {
			bad(w, "Invalid cursor.")
			return 0, nil, false
		}
		after = &queue.Cursor{Created: at, ID: id}
	}
	return limit, after, true
}
func encodeCursor(at time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(at.UTC().Format(time.RFC3339Nano) + "|" + id))
}
func (s server) createSchedule(w http.ResponseWriter, r *http.Request) {
	data, ok := readBody(w, r)
	if !ok {
		return
	}
	var in scheduler.Input
	if !decodeFields(data, map[string]any{"name": &in.Name, "cron": &in.Cron, "type": &in.Kind, "payload": &in.Payload}) {
		bad(w, "Invalid schedule submission.")
		return
	}
	v, err := s.schedules.Create(r.Context(), in)
	if err != nil {
		dbError(w, err)
		return
	}
	w.Header().Set("Location", "/v1/schedules/"+v.ID)
	write(w, 201, v)
}
func (s server) schedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuid(id) {
		bad(w, "Invalid schedule ID.")
		return
	}
	var v scheduler.Schedule
	var err error
	if r.Method == http.MethodDelete {
		v, err = s.schedules.Disable(r.Context(), id)
	} else {
		v, err = s.schedules.Get(r.Context(), id)
	}
	if err != nil {
		dbError(w, err)
		return
	}
	write(w, 200, v)
}
func (s server) listSchedules(w http.ResponseWriter, r *http.Request) {
	v, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		bad(w, "Invalid query.")
		return
	}
	for key, values := range v {
		if len(values) != 1 || values[0] == "" || (key != "limit" && key != "cursor") {
			bad(w, "Invalid query parameter.")
			return
		}
	}
	limit, after, ok := page(w, v)
	if !ok {
		return
	}
	rows, err := s.schedules.List(r.Context(), after, limit+1)
	if err != nil {
		dbError(w, err)
		return
	}
	var next *string
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[limit-1]
		token := encodeCursor(last.Created, last.ID)
		next = &token
	}
	write(w, 200, map[string]any{"schedules": rows, "next_cursor": next})
}
