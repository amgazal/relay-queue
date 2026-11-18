// Package metrics owns a single Relay process's Prometheus collectors.
package metrics

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	WebhookResults                                                                        *prometheus.CounterVec
	WebhookDuration                                                                       prometheus.Histogram
	SignatureFailures                                                                     prometheus.Counter
	Registry                                                                              *prometheus.Registry
	Submitted, Conflicts, Attempts, Completed, Failed, RenewFailures, Recovered, Timeouts prometheus.Counter
	Dispatched, DispatchFailures, Skipped                                                 prometheus.Counter
	Retries, Requests                                                                     *prometheus.CounterVec
	ClaimTime, HandlerTime, DispatchTime                                                  prometheus.Histogram
	RequestTime                                                                           *prometheus.HistogramVec
	Active                                                                                prometheus.Gauge
}

// Pass a pool on API targets to include a database-wide snapshot on scrape.
func New(pool *pgxpool.Pool) *Metrics {
	r := prometheus.NewRegistry()
	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "relay", Name: name, Help: help})
		r.MustRegister(c)
		return c
	}
	histogram := func(name, help string) prometheus.Histogram {
		h := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "relay", Name: name, Help: help, Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300}})
		r.MustRegister(h)
		return h
	}
	m := &Metrics{Registry: r,
		WebhookResults:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relay_webhook_deliveries_total", Help: "Completed handler invocations by delivery result."}, []string{"outcome"}),
		WebhookDuration:   histogram("webhook_delivery_duration_seconds", "Duration of webhook delivery invocations."),
		SignatureFailures: counter("webhook_signature_failures_total", "Rejected receiver authentication checks."),
		Submitted:         counter("jobs_submitted_total", "Newly committed job records."),
		Conflicts:         counter("submission_conflicts_total", "Conflicting submissions using an existing key."),
		Attempts:          counter("execution_attempts_total", "Handler dispatches started."),
		Completed:         counter("jobs_completed_total", "Committed successful completions."),
		Failed:            counter("failed_attempts_total", "Attempts finalized as failed."),
		RenewFailures:     counter("lease_renewal_failures_total", "Unsuccessful lease confirmations and renewals."),
		Recovered:         counter("expired_attempts_recovered_total", "Expired attempts closed by the reaper."),
		Timeouts:          counter("worker_timeouts_total", "Handler deadlines reached."),
		Dispatched:        counter("scheduler_occurrences_dispatched_total", "New scheduled jobs committed."),
		DispatchFailures:  counter("scheduler_dispatch_failures_total", "Unsuccessful scheduler operations excluding shutdown cancellation."),
		Skipped:           counter("scheduler_occurrences_skipped_total", "Intervening cron matches skipped by committed advancements."),
		ClaimTime:         histogram("claim_duration_seconds", "Elapsed time of a claim call, including empty polls."),
		HandlerTime:       histogram("handler_duration_seconds", "Time spent invoking the handler."),
		DispatchTime:      histogram("scheduler_operation_duration_seconds", "Time spent checking and dispatching one due schedule."),
		Active:            prometheus.NewGauge(prometheus.GaugeOpts{Name: "relay_active_handlers", Help: "Handler invocations currently executing."}),
		Requests:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relay_http_requests_total", Help: "Completed HTTP requests by registered route and response code. Status 401 counts authentication failures."}, []string{"route", "status"}),
		RequestTime:       prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "relay_http_request_duration_seconds", Help: "HTTP request service time."}, []string{"route"}),
		Retries:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relay_retries_total", Help: "Committed transitions that return jobs to the queue."}, []string{"reason"}),
	}
	r.MustRegister(m.WebhookResults, m.Active, m.Requests, m.RequestTime, m.Retries)
	if pool != nil {
		r.MustRegister(&snapshot{pool})
	}
	return m
}

type snapshot struct{ pool *pgxpool.Pool }

var (
	depth     = prometheus.NewDesc("relay_queue_depth", "Database-wide job count by state.", []string{"state"}, nil)
	oldest    = prometheus.NewDesc("relay_oldest_eligible_age_seconds", "Seconds since the oldest currently eligible job became eligible, or zero.", nil, nil)
	available = prometheus.NewDesc("relay_queue_snapshot_up", "One if the database snapshot was available.", nil, nil)
)

func (s *snapshot) Describe(ch chan<- *prometheus.Desc) { ch <- depth; ch <- oldest; ch <- available }
func (s *snapshot) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var counts [5]int64
	var age float64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE state='queued'),count(*) FILTER(WHERE state='running'),count(*) FILTER(WHERE state='succeeded'),count(*) FILTER(WHERE state='dead'),count(*) FILTER(WHERE state='cancelled'),
 coalesce(extract(epoch FROM now()-min(available_at) FILTER(WHERE state='queued' AND available_at<=now())),0)::float8 FROM jobs`).Scan(&counts[0], &counts[1], &counts[2], &counts[3], &counts[4], &age)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(available, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(available, prometheus.GaugeValue, 1)
	for i, state := range []string{"queued", "running", "succeeded", "dead", "cancelled"} {
		ch <- prometheus.MustNewConstMetric(depth, prometheus.GaugeValue, float64(counts[i]), state)
	}
	ch <- prometheus.MustNewConstMetric(oldest, prometheus.GaugeValue, age)
}
