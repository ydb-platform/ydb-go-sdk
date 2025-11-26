package metrics

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"

	"github.com/ydb-platform/ydb-go-sdk/v3"

	"slo/internal/log"
)

const (
	sdk        = "go"
	sdkVersion = ydb.Version
)

type (
	Metrics struct {
		mp     *sdkmetric.MeterProvider
		meter  otelmetric.Meter
		ctx    context.Context
		cancel context.CancelFunc

		// Labels for metrics
		ref   string
		label string

		// OTel instruments
		errorsTotal             otelmetric.Int64Counter
		timeoutsTotal           otelmetric.Int64Counter
		operationsTotal         otelmetric.Int64Counter
		operationsSuccessTotal  otelmetric.Int64Counter
		operationsFailureTotal  otelmetric.Int64Counter
		operationLatencySeconds otelmetric.Float64Histogram
		retryAttempts           otelmetric.Int64Gauge
		retryAttemptsTotal      otelmetric.Int64Counter
		retriesSuccessTotal     otelmetric.Int64Counter
		retriesFailureTotal     otelmetric.Int64Counter
		pendingOperations       otelmetric.Int64UpDownCounter

		// Local counters for fail-on-error logic
		errorsCount   atomic.Int64
		timeoutsCount atomic.Int64
		opsCount      atomic.Int64
	}
)

func New(endpoint, ref, label, jobName string, reportPeriodMs int) (*Metrics, error) {
	m := &Metrics{
		ref:   ref,
		label: label,
	}

	// Create context for meter provider
	m.ctx, m.cancel = context.WithCancel(context.Background())

	if endpoint == "" {
		log.Printf("Warning: no OTLP endpoint provided, metrics will not be exported")
		return m, nil
	}

	// Create OTLP HTTP exporter
	exporter, err := otlpmetrichttp.New(
		m.ctx,
		otlpmetrichttp.WithEndpoint(endpoint),
		otlpmetrichttp.WithURLPath("/api/v1/otlp/v1/metrics"),
		otlpmetrichttp.WithInsecure(), // Assuming internal network
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Create resource with labels
	res, err := resource.New(
		m.ctx,
		resource.WithAttributes(
			semconv.ServiceName(jobName),
			attribute.String("ref", ref),
			attribute.String("sdk", fmt.Sprintf("%s-%s", sdk, label)),
			attribute.String("sdk_version", sdkVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create meter provider with configurable export interval
	exportInterval := time.Duration(reportPeriodMs) * time.Millisecond
	if exportInterval == 0 {
		exportInterval = 250 * time.Millisecond // Default 250ms
	}

	m.mp = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(
				exporter,
				sdkmetric.WithInterval(exportInterval),
			),
		),
	)

	// Create meter
	m.meter = m.mp.Meter("slo-workload")

	// Initialize counters
	m.errorsTotal, err = m.meter.Int64Counter(
		"sdk.errors.total",
		otelmetric.WithDescription("Total number of errors encountered, categorized by error type"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create errorsTotal counter: %w", err)
	}

	m.timeoutsTotal, err = m.meter.Int64Counter(
		"sdk.timeouts.total",
		otelmetric.WithDescription("Total number of timeout errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create timeoutsTotal counter: %w", err)
	}

	m.operationsTotal, err = m.meter.Int64Counter(
		"sdk.operations.total",
		otelmetric.WithDescription("Total number of operations, categorized by type"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create operationsTotal counter: %w", err)
	}

	m.operationsSuccessTotal, err = m.meter.Int64Counter(
		"sdk.operations.success.total",
		otelmetric.WithDescription("Total number of successful operations, categorized by type"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create operationsSuccessTotal counter: %w", err)
	}

	m.operationsFailureTotal, err = m.meter.Int64Counter(
		"sdk.operations.failure.total",
		otelmetric.WithDescription("Total number of failed operations, categorized by type"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create operationsFailureTotal counter: %w", err)
	}

	m.operationLatencySeconds, err = m.meter.Float64Histogram(
		"sdk.operation.latency.seconds",
		otelmetric.WithDescription("Latency of operations performed by the SDK in seconds"),
		otelmetric.WithExplicitBucketBoundaries(
			0.001, 0.002, 0.003, 0.004, 0.005, 0.0075,
			0.010, 0.020, 0.050, 0.100, 0.200, 0.500, 1.000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create operationLatencySeconds histogram: %w", err)
	}

	m.retryAttemptsTotal, err = m.meter.Int64Counter(
		"sdk.retry.attempts.total",
		otelmetric.WithDescription("Total number of retry attempts, categorized by operation type"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create retryAttemptsTotal counter: %w", err)
	}

	m.retriesSuccessTotal, err = m.meter.Int64Counter(
		"sdk.retries.success.total",
		otelmetric.WithDescription("Total number of successful retries, categorized by operation type"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create retriesSuccessTotal counter: %w", err)
	}

	m.retriesFailureTotal, err = m.meter.Int64Counter(
		"sdk.retries.failure.total",
		otelmetric.WithDescription("Total number of failed retries, categorized by operation type"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create retriesFailureTotal counter: %w", err)
	}

	// Create synchronous gauges
	m.retryAttempts, err = m.meter.Int64Gauge(
		"sdk.retry.attempts",
		otelmetric.WithDescription("Current retry attempts, categorized by operation type"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create retryAttempts gauge: %w", err)
	}

	m.pendingOperations, err = m.meter.Int64UpDownCounter(
		"sdk.pending.operations",
		otelmetric.WithDescription("Current number of pending operations, categorized by type"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create pendingOperations counter: %w", err)
	}

	return m, nil
}

func (m *Metrics) Push() error {
	if m.mp == nil {
		return nil
	}
	// Force flush
	return m.mp.ForceFlush(m.ctx)
}

func (m *Metrics) Reset() error {
	// Reset local counters for fail-on-error logic
	m.errorsCount.Store(0)
	m.timeoutsCount.Store(0)
	m.opsCount.Store(0)

	// Note: OTel counters/gauges are cumulative and cannot be reset
	// This is just for local state
	return m.Push()
}

func (m *Metrics) Close() error {
	if m.mp == nil {
		return nil
	}
	m.cancel()
	return m.mp.Shutdown(context.Background())
}

// commonAttrs returns common attributes for all metrics
func (m *Metrics) commonAttrs(additional ...attribute.KeyValue) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("ref", m.ref),
	}
	return append(attrs, additional...)
}

func (m *Metrics) Start(name SpanName) Span {
	j := Span{
		name:  name,
		start: time.Now(),
		m:     m,
	}

	if m.meter != nil {
		m.pendingOperations.Add(m.ctx, 1, otelmetric.WithAttributes(
			m.commonAttrs(attribute.String("operation_type", name))...,
		))
	}

	return j
}

func (j Span) Finish(err error, attempts int) {
	if j.m.meter == nil {
		return
	}

	latency := time.Since(j.start)

	attrs := j.m.commonAttrs(attribute.String("operation_type", j.name))

	// Decrement pending operations
	j.m.pendingOperations.Add(j.m.ctx, -1, otelmetric.WithAttributes(attrs...))

	// Record retry attempts gauge
	j.m.retryAttempts.Record(j.m.ctx, int64(attempts), otelmetric.WithAttributes(attrs...))

	// Increment counters
	j.m.operationsTotal.Add(j.m.ctx, 1, otelmetric.WithAttributes(attrs...))
	j.m.retryAttemptsTotal.Add(j.m.ctx, int64(attempts), otelmetric.WithAttributes(attrs...))

	// Local counters for fail-on-error
	j.m.opsCount.Add(1)

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			j.m.timeoutsTotal.Add(j.m.ctx, 1, otelmetric.WithAttributes(attrs...))
			j.m.timeoutsCount.Add(1)
		}
		j.m.errorsTotal.Add(j.m.ctx, 1, otelmetric.WithAttributes(
			j.m.commonAttrs(attribute.String("error_type", err.Error()))...,
		))
		j.m.errorsCount.Add(1)

		j.m.retriesFailureTotal.Add(j.m.ctx, int64(attempts), otelmetric.WithAttributes(attrs...))
		j.m.operationsFailureTotal.Add(j.m.ctx, 1, otelmetric.WithAttributes(attrs...))
		j.m.operationLatencySeconds.Record(j.m.ctx, latency.Seconds(), otelmetric.WithAttributes(
			j.m.commonAttrs(
				attribute.String("operation_type", j.name),
				attribute.String("operation_status", OperationStatusFailue),
			)...,
		))
	} else {
		j.m.retriesSuccessTotal.Add(j.m.ctx, int64(attempts), otelmetric.WithAttributes(attrs...))
		j.m.operationsSuccessTotal.Add(j.m.ctx, 1, otelmetric.WithAttributes(attrs...))
		j.m.operationLatencySeconds.Record(j.m.ctx, latency.Seconds(), otelmetric.WithAttributes(
			j.m.commonAttrs(
				attribute.String("operation_type", j.name),
				attribute.String("operation_status", OperationStatusSuccess),
			)...,
		))
	}
}

func (m *Metrics) OperationsTotal() float64 {
	return float64(m.opsCount.Load())
}

func (m *Metrics) ErrorsTotal() float64 {
	return float64(m.errorsCount.Load())
}

func (m *Metrics) TimeoutsTotal() float64 {
	return float64(m.timeoutsCount.Load())
}

func (m *Metrics) FailOnError() {
	if m.ErrorsTotal()*100 > m.OperationsTotal() {
		log.Panicf(
			"unretriable (or not successfully retried) errors: %.0f errors out of %.0f operations",
			m.ErrorsTotal(),
			m.OperationsTotal(),
		)
	}
	if m.TimeoutsTotal()*100 > m.OperationsTotal() {
		log.Panicf(
			"user timeouts: %.0f timeouts out of %.0f operations",
			m.TimeoutsTotal(),
			m.OperationsTotal(),
		)
	}
}
