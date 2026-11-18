// Package webhook implements signed JSON event delivery to operator-configured endpoints.
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
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/amgazal/relay-queue/internal/metrics"
	"github.com/amgazal/relay-queue/internal/worker"
)

const Kind = "webhook.deliver"
const bodyLimit = 1 << 20
const responseLimit = 4096

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var uuid = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type Event struct {
	ID   string          `json:"event_id"`
	Type string          `json:"event_type"`
	Data json.RawMessage `json:"data"`
}

func parse(raw []byte, job bool) (Event, string, bool) {
	var e Event
	var dest string
	fields := map[string]any{"event_id": &e.ID, "event_type": &e.Type, "data": &e.Data}
	if job {
		fields["destination"] = &dest
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	tok, err := d.Token()
	if !utf8.Valid(raw) || err != nil || tok != json.Delim('{') {
		return e, "", false
	}
	for d.More() {
		k, err := d.Token()
		if err != nil {
			return e, "", false
		}
		name, ok := k.(string)
		v, known := fields[name]
		if !ok || !known || d.Decode(v) != nil {
			return e, "", false
		}
		delete(fields, name)
	}
	_, err = d.Token()
	return e, dest, err == nil && len(fields) == 0 && d.Decode(new(any)) == io.EOF && uuid.MatchString(e.ID) && identifier.MatchString(e.Type) && (!job || identifier.MatchString(dest))
}

type destination struct {
	URL    string `json:"url"`
	Secret string `json:"secret"`
}
type Sender struct {
	targets map[string]destination
	client  *http.Client
	m       *metrics.Metrics
}

func NewSender(config string, m *metrics.Metrics) (*Sender, error) {
	targets := map[string]destination{}
	if config != "" {
		d := json.NewDecoder(strings.NewReader(config))
		d.DisallowUnknownFields()
		if d.Decode(&targets) != nil || targets == nil || d.Decode(new(any)) != io.EOF {
			return nil, errors.New("invalid webhook destination configuration")
		}
	}
	for id, t := range targets {
		u, err := url.Parse(t.URL)
		if err != nil || !identifier.MatchString(id) || len(t.Secret) < 32 || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" {
			return nil, errors.New("invalid webhook destination configuration")
		}
		if port := u.Port(); port != "" {
			n, err := strconv.ParseUint(port, 10, 16)
			if err != nil || n == 0 {
				return nil, errors.New("invalid webhook destination port")
			}
		}
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "https" && !(u.Scheme == "http" && ip != nil && ip.IsLoopback()) {
			return nil, errors.New("webhooks require HTTPS or a loopback HTTP address")
		}
	}
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext, TLSHandshakeTimeout: 3 * time.Second, ResponseHeaderTimeout: 5 * time.Second, IdleConnTimeout: 30 * time.Second, MaxResponseHeaderBytes: 16 << 10}
	return &Sender{targets, &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, m}, nil
}
func (s *Sender) Close() { s.client.CloseIdleConnections() }
func mac(secret, stamp, id string, body []byte) []byte {
	h := hmac.New(sha256.New, []byte(secret))
	io.WriteString(h, stamp+"."+id+".")
	h.Write(body)
	return h.Sum(nil)
}

func (s *Sender) Deliver(ctx context.Context, raw json.RawMessage) (err error) {
	start := time.Now()
	defer func() {
		outcome := "success"
		if errors.Is(err, worker.ErrPermanent) {
			outcome = "permanent"
		} else if err != nil {
			outcome = "transient"
		}
		s.m.WebhookResults.WithLabelValues(outcome).Inc()
		s.m.WebhookDuration.Observe(time.Since(start).Seconds())
	}()
	e, id, ok := parse(raw, true)
	if !ok {
		return fmt.Errorf("%w: invalid webhook event", worker.ErrPermanent)
	}
	target, ok := s.targets[id]
	if !ok {
		return fmt.Errorf("%w: unknown webhook destination", worker.ErrPermanent)
	}
	body, err := json.Marshal(e)
	if err != nil || len(body) > bodyLimit {
		return fmt.Errorf("%w: invalid webhook body", worker.ErrPermanent)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: invalid webhook request", worker.ErrPermanent)
	}
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Relay-Event-ID", e.ID)
	req.Header.Set("X-Relay-Timestamp", stamp)
	req.Header.Set("X-Relay-Signature", "sha256="+hex.EncodeToString(mac(target.Secret, stamp, e.ID, body)))
	res, err := s.client.Do(req)
	if err != nil {
		return errors.New("webhook transport failure")
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, responseLimit))
	code := res.StatusCode
	switch {
	case code >= 200 && code < 300:
		return nil
	case code == 408 || code == 429 || code >= 500 && code < 600:
		return fmt.Errorf("webhook HTTP %d", code)
	default:
		return fmt.Errorf("%w: webhook HTTP %d", worker.ErrPermanent, code)
	}
}
