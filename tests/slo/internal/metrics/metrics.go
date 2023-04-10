package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

const (
	sdk        = "go"
	sdkVersion = ydb.Version
)

type Metrics struct {
	oks       *prometheus.GaugeVec
	notOks    *prometheus.GaugeVec
	inflight  *prometheus.GaugeVec
	latencies *prometheus.SummaryVec

	p *push.Pusher

	jobs      map[uuid.UUID]job
	jobsMutex sync.RWMutex

	label string
}

func NewMetrics(url string, label string) (m *Metrics, err error) {
	m = &Metrics{
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

	m.jobs = make(map[uuid.UUID]job)

	m.p = push.New(url, "workload-go").
		Grouping("sdk", fmt.Sprintf("%s-%s", sdk, m.label)).
		Grouping("sdkVersion", sdkVersion).
		Collector(m.oks).
		Collector(m.notOks).
		Collector(m.inflight).
		Collector(m.latencies)

	return m, err
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

func (m *Metrics) StartJob(name JobName) (id uuid.UUID) {
	id = uuid.New()
	j := job{
		name:  name,
		start: time.Now(),
	}

	m.jobsMutex.Lock()
	m.jobs[id] = j
	m.jobsMutex.Unlock()

	m.inflight.WithLabelValues(name).Add(1)

	return id
}

func (m *Metrics) StopJob(id uuid.UUID, ok bool) {
	m.jobsMutex.Lock()
	defer m.jobsMutex.Unlock()
	j, found := m.jobs[id]
	if !found {
		return
	}
	delete(m.jobs, id)

	m.inflight.WithLabelValues(j.name).Sub(1)

	latency := float64(time.Since(j.start).Milliseconds())

	if ok {
		m.oks.WithLabelValues(j.name).Add(1)
		m.latencies.WithLabelValues("ok", j.name).Observe(latency)
		return
	}
	m.notOks.WithLabelValues(j.name).Add(1)
	m.latencies.WithLabelValues("err", j.name).Observe(latency)
}

func (m *Metrics) ActiveJobsCount() int {
	return len(m.jobs)
}
