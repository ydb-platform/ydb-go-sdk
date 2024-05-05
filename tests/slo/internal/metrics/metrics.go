package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

const (
	sdk        = "go"
	sdkVersion = ydb.Version
)

type (
	Metrics struct {
		oks       *prometheus.GaugeVec
		notOks    *prometheus.GaugeVec
		inflight  *prometheus.GaugeVec
		latencies *prometheus.SummaryVec
		attempts  *prometheus.HistogramVec

		p *push.Pusher

		label string
	}
)

func New(url, label, jobName string) (*Metrics, error) {
	m := &Metrics{
		label:     label,
		oks:       nil,
		notOks:    nil,
		inflight:  nil,
		latencies: nil,
		attempts:  nil,
		p:         nil,
	}

	m.oks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "oks",
			Help:        "amount of OK requests",
			Namespace:   "",
			Subsystem:   "",
			ConstLabels: nil,
		},
		[]string{"jobName"},
	)
	m.notOks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "not_oks",
			Help:        "amount of not OK requests",
			Namespace:   "",
			Subsystem:   "",
			ConstLabels: nil,
		},
		[]string{"jobName"},
	)
	m.inflight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "inflight",
			Help:        "amount of requests in flight",
			Namespace:   "",
			Subsystem:   "",
			ConstLabels: nil,
		},
		[]string{"jobName"},
	)
	m.latencies = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:   "",
			Subsystem:   "",
			Name:        "latency",
			Help:        "summary of latencies in ms",
			ConstLabels: nil,
			Objectives: map[float64]float64{
				0.5:  0,
				0.99: 0,
				1.0:  0,
			},
			MaxAge:     15 * time.Second, //nolint:gomnd
			AgeBuckets: 0,
			BufCap:     0,
		},
		[]string{"status", "jobName"},
	)
	m.attempts = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:                       "",
			Subsystem:                       "",
			Name:                            "attempts",
			Help:                            "summary of amount for request",
			ConstLabels:                     nil,
			Buckets:                         prometheus.LinearBuckets(1, 1, 10), //nolint:gomnd
			NativeHistogramBucketFactor:     0,
			NativeHistogramZeroThreshold:    0,
			NativeHistogramMaxBucketNumber:  0,
			NativeHistogramMinResetDuration: 0,
			NativeHistogramMaxZeroThreshold: 0,
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

	latency := time.Since(j.start)

	if attempts > 1 {
		fmt.Printf("more than 1 attempt for request (request_type: %q, attempts: %d, start: %s, latency: %s, err: %v)\n",
			j.name,
			attempts,
			j.start.Format(time.DateTime),
			latency.String(),
			err,
		)
	}

	var (
		successLabel   = JobStatusOK
		successCounter = j.m.oks
	)

	if err != nil {
		successLabel = JobStatusErr
		successCounter = j.m.notOks
	}

	j.m.latencies.WithLabelValues(successLabel, j.name).Observe(float64(latency.Milliseconds()))
	j.m.attempts.WithLabelValues(successLabel, j.name).Observe(float64(attempts))
	successCounter.WithLabelValues(j.name).Add(1)
}
