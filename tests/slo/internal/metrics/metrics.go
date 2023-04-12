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

		p *push.Pusher

		label string
	}
)

func New(url string, label string) (*Metrics, error) {
	m := &Metrics{
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
				0.5:   0.05,
				0.9:   0.01,
				0.95:  0.005,
				0.99:  0.001,
				0.999: 0.0001,
			},
		},
		[]string{"status", "jobName"},
	)

	m.p = push.New(url, "workload-go").
		Grouping("sdk", fmt.Sprintf("%s-%s", sdk, m.label)).
		Grouping("sdkVersion", sdkVersion).
		Collector(m.oks).
		Collector(m.notOks).
		Collector(m.inflight).
		Collector(m.latencies)

	return m, m.Reset()
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

func (j Span) Stop(err error) {
	j.m.inflight.WithLabelValues(j.name).Sub(1)

	latency := float64(time.Since(j.start).Milliseconds())

	if err != nil {
		j.m.notOks.WithLabelValues(j.name).Add(1)
		j.m.latencies.WithLabelValues("err", j.name).Observe(latency)
		return
	}

	j.m.oks.WithLabelValues(j.name).Add(1)
	j.m.latencies.WithLabelValues("ok", j.name).Observe(latency)
}
