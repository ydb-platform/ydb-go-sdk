package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"
)

const (
	jobName    = "workload-go"
	sdk        = "go"
	sdkVersion = ydb.Version
)

type (
	Metrics struct {
		oks        *prometheus.GaugeVec
		notOks     *prometheus.GaugeVec
		inflight   *prometheus.GaugeVec
		latencies  *prometheus.SummaryVec
		latenciesH *prometheus.HistogramVec
		attempts   *prometheus.HistogramVec

		p *push.Pusher

		logger *zap.Logger

		label string
	}
)

func New(logger *zap.Logger, url, label string) (*Metrics, error) {
	m := &Metrics{
		logger: logger.Named("metrics"),

		label: label,
	}

	m.oks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "oks",
			Help: "amount of OK requests",
		},
		[]string{"jobName"},
	)
	m.notOks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "not_oks",
			Help: "amount of not OK requests",
		},
		[]string{"jobName"},
	)
	m.inflight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "inflight",
			Help: "amount of requests in flight",
		},
		[]string{"jobName"},
	)
	m.latencies = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "latency",
			Help: "summary of latencies in ms",
			Objectives: map[float64]float64{
				0.5:  0.05,
				0.99: 0.001,
				1.0:  0.0,
			},
			MaxAge: 15 * time.Second,
		},
		[]string{"status", "jobName"},
	)
	m.latenciesH = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "latencyH",
			Help:    "latencies in ms",
			Buckets: []float64{1, 2, 5, 10, 20, 50, 100, 200, 400, 800, 1600, 3200, 6400},
		},
		[]string{"status", "jobName"},
	)
	m.attempts = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "attempts",
			Help:    "amount of retries for request",
			Buckets: prometheus.LinearBuckets(1, 1, 10),
		},
		[]string{"status", "jobName"},
	)

	m.p = push.New(url, jobName).
		Grouping("sdk", fmt.Sprintf("%s-%s", sdk, m.label)).
		Grouping("sdkVersion", sdkVersion).
		Collector(m.oks).
		Collector(m.notOks).
		Collector(m.inflight).
		Collector(m.latencies).
		Collector(m.latenciesH).
		Collector(m.attempts)

	return m, m.Reset() //nolint:gocritic
}

func (m *Metrics) Push() error {
	return m.p.Push()
}

func (m *Metrics) Reset() error {
	m.oks.WithLabelValues(JobRead).Set(0)
	m.oks.WithLabelValues(JobWrite).Set(0)

	m.notOks.WithLabelValues(JobRead).Set(0)
	m.notOks.WithLabelValues(JobWrite).Set(0)

	m.inflight.WithLabelValues(JobRead).Set(0)
	m.inflight.WithLabelValues(JobWrite).Set(0)

	m.latencies.Reset()
	m.latenciesH.Reset()

	m.attempts.Reset()

	return m.Push()
}

func (m *Metrics) Start(name SpanName) Span {
	j := Span{
		name:  name,
		start: time.Now(),
		m:     m,
	}

	m.inflight.WithLabelValues(name).Add(1)

	return j
}

func (j Span) Stop(err error, attempts int) {
	j.m.inflight.WithLabelValues(j.name).Sub(1)

	l := time.Since(j.start).Milliseconds()

	if attempts > 1 {
		j.m.logger.Warn("more than 1 attempt for request",
			zap.String("request type", j.name),
			zap.Int("attempts", attempts),
			zap.Time("start", j.start),
			zap.Int64("latency", l),
			zap.Error(err),
		)
	}

	latency := float64(l)

	if err != nil {
		j.m.notOks.WithLabelValues(j.name).Add(1)
		j.m.latencies.WithLabelValues(JobStatusErr, j.name).Observe(latency)
		j.m.latenciesH.WithLabelValues(JobStatusErr, j.name).Observe(latency)
		j.m.attempts.WithLabelValues(JobStatusErr, j.name).Observe(float64(attempts))
		return
	}

	j.m.oks.WithLabelValues(j.name).Add(1)
	j.m.latencies.WithLabelValues(JobStatusOK, j.name).Observe(latency)
	j.m.latenciesH.WithLabelValues(JobStatusOK, j.name).Observe(latency)
	j.m.attempts.WithLabelValues(JobStatusOK, j.name).Observe(float64(attempts))
}
