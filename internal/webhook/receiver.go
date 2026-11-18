package webhook

import (
	"crypto/hmac"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/amgazal/relay-queue/internal/metrics"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Receiver(pool *pgxpool.Pool, secret string, failFirst bool, m *metrics.Metrics) (http.Handler, error) {
	if len(secret) < 32 {
		return nil, errors.New("receiver signing secret requires at least 32 bytes")
	}
	var fail atomic.Bool
	fail.Store(failFirst)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			w.WriteHeader(415)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, bodyLimit))
		if err != nil {
			w.WriteHeader(413)
			return
		}
		stamp, id := r.Header.Get("X-Relay-Timestamp"), r.Header.Get("X-Relay-Event-ID")
		unix, err := strconv.ParseInt(stamp, 10, 64)
		now := time.Now().Unix()
		sig, prefix := strings.CutPrefix(r.Header.Get("X-Relay-Signature"), "sha256=")
		digest, decodeErr := hex.DecodeString(sig)
		if err != nil || unix < now-300 || unix > now+300 || !prefix || decodeErr != nil || !uuid.MatchString(id) || !hmac.Equal(mac(secret, stamp, id, body), digest) {
			m.SignatureFailures.Inc()
			w.WriteHeader(401)
			return
		}
		e, _, ok := parse(body, false)
		if !ok || e.ID != id {
			w.WriteHeader(400)
			return
		}
		if fail.Swap(false) {
			w.WriteHeader(503)
			return
		}
		_, err = pool.Exec(r.Context(), "INSERT INTO demo_received_events(event_id,event_type,data) VALUES($1,$2,$3) ON CONFLICT(event_id) DO NOTHING", e.ID, e.Type, e.Data)
		if err != nil {
			var pg *pgconn.PgError
			if errors.As(err, &pg) && strings.HasPrefix(pg.Code, "22") {
				w.WriteHeader(400)
				return
			}
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	})
	return mux, nil
}
