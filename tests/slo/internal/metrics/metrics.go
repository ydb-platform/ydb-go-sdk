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
		p     *push.Pusher
		ref   string
		label string

		errorsTotal *prometheus.CounterVec

		operationsTotal         *prometheus.CounterVec
		operationsSuccessTotal  *prometheus.CounterVec
		operationsFailureTotal  *prometheus.CounterVec
		operationLatencySeconds *prometheus.HistogramVec

		retryAttemptsTotal  *prometheus.CounterVec
		retriesSuccessTotal *prometheus.CounterVec
		retriesFailureTotal *prometheus.CounterVec

		pendingOperations *prometheus.GaugeVec
		// TODO:
		// sdk_cpu_usage_seconds_total *prometheus.CounterVec
		// sdk_memory_usage_bytes *prometheus.GaugeVec
		// sdk_connections_open *prometheus.GaugeVec
		// sdk_load_balancing_decisions_total *prometheus.CounterVec
		// sdk_requests_by_node_total *prometheus.CounterVec
		// sdk_timeouts_total *prometheus.CounterVec
		// sdk_connection_errors_total *prometheus.CounterVec
		// sdk_circuit_breaker_trips_total *prometheus.CounterVec
		// sdk_request_queue_size_total *prometheus.CounterVec
		// sdk_throttling_events_total *prometheus.CounterVec
		// sdk_current_load_factor *prometheus.GaugeVec
	}
)

func New(url, ref, label, jobName string) (*Metrics, error) {
	m := &Metrics{
		ref:   ref,
		label: label,
	}

	m.errorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{ //nolint:promlinter
			Name: "sdk_errors_total",
			Help: "Total number of errors encountered, categorized by error type.",
		},
		[]string{"error_type"},
	)

	m.operationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{ //nolint:promlinter
			Name: "sdk_operations_total",
			Help: "Total number of operations, categorized by type attempted by the SDK.",
		},
		[]string{"operation_type"},
	)

	m.operationsSuccessTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{ //nolint:promlinter
			Name: "sdk_operations_success_total",
			Help: "Total number of successful operations, categorized by type.",
		},
		[]string{"operation_type"},
	)

	m.operationsFailureTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{ //nolint:promlinter
			Name: "sdk_operations_failure_total",
			Help: "Total number of failed operations, categorized by type.",
		},
		[]string{"operation_type"},
	)

	m.operationLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{ //nolint:promlinter
			Name: "sdk_operation_latency_seconds",
			Help: "Latency of operations performed by the SDK in seconds, categorized by type and status.",
			Buckets: []float64{
				0.001,  // 1 ms
				0.002,  // 2 ms
				0.003,  // 3 ms
				0.004,  // 4 ms
				0.005,  // 5 ms
				0.0075, // 7.5 ms
				0.010,  // 10 ms
				0.020,  // 20 ms
				0.050,  // 50 ms
				0.100,  // 100 ms
				0.200,  // 200 ms
				0.500,  // 500 ms
				1.000,  // 1 s
			},
		},
		[]string{"operation_type", "operation_status"},
	)

	m.retryAttemptsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{ //nolint:promlinter
			Name: "sdk_retry_attempts_total",
			Help: "Total number of retry attempts, categorized by operation type.",
		},
		[]string{"operation_type"},
	)

	m.retriesSuccessTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{ //nolint:promlinter
			Name: "sdk_retries_success_total",
			Help: "Total number of successful retries, categorized by operation type.",
		},
		[]string{"operation_type"},
	)

	m.retriesFailureTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{ //nolint:promlinter
			Name: "sdk_retries_failure_total",
			Help: "Total number of failed retries, categorized by operation type.",
		},
		[]string{"operation_type"},
	)

	m.pendingOperations = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{ //nolint:promlinter
			Name: "sdk_pending_operations",
			Help: "Current number of pending operations, categorized by type.",
		},
		[]string{"operation_type"},
	)

	m.p = push.New(url, jobName).
		Grouping("ref", m.ref).
		Grouping("sdk", fmt.Sprintf("%s-%s", sdk, m.label)).
		Grouping("sdk_version", sdkVersion).
		Collector(m.operationsTotal).
		Collector(m.operationsSuccessTotal).
		Collector(m.operationsFailureTotal).
		Collector(m.operationLatencySeconds).
		Collector(m.retryAttemptsTotal).
		Collector(m.retriesSuccessTotal).
		Collector(m.retriesFailureTotal).
		Collector(m.pendingOperations)

	return m, m.Reset() //nolint:gocritic
}

func (m *Metrics) Push() error {
	return m.p.Push()
}

func (m *Metrics) Reset() error {
	m.errorsTotal.Reset()

	m.operationsTotal.Reset()
	m.operationsSuccessTotal.Reset()
	m.operationsFailureTotal.Reset()
	m.operationLatencySeconds.Reset()

	m.retryAttemptsTotal.Reset()
	m.retriesSuccessTotal.Reset()
	m.retriesFailureTotal.Reset()

	m.pendingOperations.Reset()

	return m.Push()
}

func (m *Metrics) Start(name SpanName) Span {
	j := Span{
		name:  name,
		start: time.Now(),
		m:     m,
	}

	m.pendingOperations.WithLabelValues(name).Add(1)

	return j
}

func (j Span) Finish(err error, attempts int) {
	latency := time.Since(j.start)
	j.m.pendingOperations.WithLabelValues(j.name).Sub(1)

	j.m.operationsTotal.WithLabelValues(j.name).Add(1)
	j.m.retryAttemptsTotal.WithLabelValues(j.name).Add(float64(attempts))

	if err != nil {
		j.m.errorsTotal.WithLabelValues(err.Error()).Add(1)
		// j.m.retriesFailureTotal.WithLabelValues(j.name).Add(1)
		j.m.operationsFailureTotal.WithLabelValues(j.name).Add(1)
		j.m.operationLatencySeconds.WithLabelValues(j.name, OperationStatusFailue).Observe(latency.Seconds())
	} else {
		j.m.operationsSuccessTotal.WithLabelValues(j.name).Add(1)
		// j.m.retriesSuccessTotal.WithLabelValues(j.name).Add(1)
		j.m.operationLatencySeconds.WithLabelValues(j.name, OperationStatusSuccess).Observe(latency.Seconds())
	}
}
