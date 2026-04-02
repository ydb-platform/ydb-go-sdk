package framework

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

const (
	sdk        = "go"
	sdkVersion = ydb.Version

	hdrMinMicroseconds  = 1
	hdrMaxMicroseconds  = 60_000_000
	hdrSignificantDigits = 5
)

type SpanName = string

const (
	OperationTypeRead  SpanName = "read"
	OperationTypeWrite SpanName = "write"
)

const (
	OperationStatusSuccess = "success"
	OperationStatusFailure = "failure"
)

type Span struct {
	name  SpanName
	start time.Time
	m     *Metrics
}

type latencyHistogram struct {
	mu         sync.RWMutex
	hdrh       *hdrhistogram.Histogram
	histograms map[attribute.Set]*hdrhistogram.Histogram
}

func newLatencyHistogram() *latencyHistogram {
	return &latencyHistogram{
		hdrh:       hdrhistogram.New(hdrMinMicroseconds, hdrMaxMicroseconds, hdrSignificantDigits),
		histograms: make(map[attribute.Set]*hdrhistogram.Histogram),
	}
}

func (h *latencyHistogram) Record(latencyMicroseconds int64, attrs attribute.Set) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.hdrh.RecordValue(latencyMicroseconds)

	hist, ok := h.histograms[attrs]
	if !ok {
		hist = hdrhistogram.New(hdrMinMicroseconds, hdrMaxMicroseconds, hdrSignificantDigits)
		h.histograms[attrs] = hist
	}
	hist.RecordValue(latencyMicroseconds)
}

func (h *latencyHistogram) GetPercentilesByAttrs() map[attribute.Set][]int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make(map[attribute.Set][]int64, len(h.histograms))
	for attrs, hist := range h.histograms {
		result[attrs] = []int64{
			hist.ValueAtQuantile(.5),
			hist.ValueAtQuantile(.95),
			hist.ValueAtQuantile(.99),
		}
	}

	return result
}

type Metrics struct {
	mp    *sdkmetric.MeterProvider
	meter otelmetric.Meter

	ref   string
	label string

	latency    *latencyHistogram
	latencyP50 otelmetric.Float64ObservableGauge
	latencyP95 otelmetric.Float64ObservableGauge
	latencyP99 otelmetric.Float64ObservableGauge

	errorsTotal        otelmetric.Int64Counter
	timeoutsTotal      otelmetric.Int64Counter
	operationsTotal    otelmetric.Int64Counter
	retryAttemptsTotal otelmetric.Int64Counter
	retryAttempts      otelmetric.Int64Gauge
	pendingOperations  otelmetric.Int64UpDownCounter
}

func NewMetrics(ctx context.Context, cfg *Config) (*Metrics, error) {
	m := &Metrics{
		ref:     cfg.Ref,
		label:   cfg.Label,
		latency: newLatencyHistogram(),
	}

	if cfg.OTLPEndpoint == "" {
		return m, nil
	}

	exporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.Label),
			attribute.String("ref", cfg.Ref),
			attribute.String("sdk", sdk),
			attribute.String("sdk_version", sdkVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	m.mp = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(
				exporter,
				sdkmetric.WithInterval(1*time.Second),
			),
		),
	)

	m.meter = m.mp.Meter(fmt.Sprintf("slo-workload-%s-%s", cfg.Label, cfg.Ref))

	if err = m.initInstruments(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Metrics) initInstruments() error {
	var err error

	m.latencyP50, err = m.meter.Float64ObservableGauge(
		"sdk.operation.latency.p50.seconds",
		otelmetric.WithUnit("s"),
		otelmetric.WithDescription("50th percentile latency of operations in seconds"),
	)
	if err != nil {
		return fmt.Errorf("failed to create latencyP50 observable gauge: %w", err)
	}

	m.latencyP95, err = m.meter.Float64ObservableGauge(
		"sdk.operation.latency.p95.seconds",
		otelmetric.WithUnit("s"),
		otelmetric.WithDescription("95th percentile latency of operations in seconds"),
	)
	if err != nil {
		return fmt.Errorf("failed to create latencyP95 observable gauge: %w", err)
	}

	m.latencyP99, err = m.meter.Float64ObservableGauge(
		"sdk.operation.latency.p99.seconds",
		otelmetric.WithUnit("s"),
		otelmetric.WithDescription("99th percentile latency of operations in seconds"),
	)
	if err != nil {
		return fmt.Errorf("failed to create latencyP99 observable gauge: %w", err)
	}

	m.errorsTotal, err = m.meter.Int64Counter(
		"sdk.errors.total",
		otelmetric.WithUnit("{error}"),
		otelmetric.WithDescription("Total number of errors encountered, categorized by error type"),
	)
	if err != nil {
		return fmt.Errorf("failed to create errorsTotal counter: %w", err)
	}

	m.timeoutsTotal, err = m.meter.Int64Counter(
		"sdk.timeouts.total",
		otelmetric.WithUnit("{timeout}"),
		otelmetric.WithDescription("Total number of timeout errors"),
	)
	if err != nil {
		return fmt.Errorf("failed to create timeoutsTotal counter: %w", err)
	}

	m.operationsTotal, err = m.meter.Int64Counter(
		"sdk.operations.total",
		otelmetric.WithUnit("{operation}"),
		otelmetric.WithDescription("Total number of operations, categorized by type"),
	)
	if err != nil {
		return fmt.Errorf("failed to create operationsTotal counter: %w", err)
	}

	m.retryAttemptsTotal, err = m.meter.Int64Counter(
		"sdk.retry.attempts.total",
		otelmetric.WithUnit("{attempt}"),
		otelmetric.WithDescription("Total number of retry attempts, categorized by operation type and status"),
	)
	if err != nil {
		return fmt.Errorf("failed to create retryAttemptsTotal counter: %w", err)
	}

	m.retryAttempts, err = m.meter.Int64Gauge(
		"sdk.retry.attempts",
		otelmetric.WithDescription("Current retry attempts, categorized by operation type and status"),
	)
	if err != nil {
		return fmt.Errorf("failed to create retryAttempts gauge: %w", err)
	}

	m.pendingOperations, err = m.meter.Int64UpDownCounter(
		"sdk.pending.operations",
		otelmetric.WithDescription("Current number of pending operations, categorized by type"),
	)
	if err != nil {
		return fmt.Errorf("failed to create pendingOperations counter: %w", err)
	}

	_, err = m.meter.RegisterCallback(func(ctx context.Context, observer otelmetric.Observer) error {
		percentiles := m.latency.GetPercentilesByAttrs()
		for attrs, vals := range percentiles {
			a := otelmetric.WithAttributes(attrs.ToSlice()...)
			observer.ObserveFloat64(m.latencyP50, float64(vals[0])/1e6, a)
		}

		return nil
	}, m.latencyP50)
	if err != nil {
		return fmt.Errorf("failed to register callback for latencyP50: %w", err)
	}

	_, err = m.meter.RegisterCallback(func(ctx context.Context, observer otelmetric.Observer) error {
		percentiles := m.latency.GetPercentilesByAttrs()
		for attrs, vals := range percentiles {
			a := otelmetric.WithAttributes(attrs.ToSlice()...)
			observer.ObserveFloat64(m.latencyP95, float64(vals[1])/1e6, a)
		}

		return nil
	}, m.latencyP95)
	if err != nil {
		return fmt.Errorf("failed to register callback for latencyP95: %w", err)
	}

	_, err = m.meter.RegisterCallback(func(ctx context.Context, observer otelmetric.Observer) error {
		percentiles := m.latency.GetPercentilesByAttrs()
		for attrs, vals := range percentiles {
			a := otelmetric.WithAttributes(attrs.ToSlice()...)
			observer.ObserveFloat64(m.latencyP99, float64(vals[2])/1e6, a)
		}

		return nil
	}, m.latencyP99)
	if err != nil {
		return fmt.Errorf("failed to register callback for latencyP99: %w", err)
	}

	return nil
}

func (m *Metrics) Meter() otelmetric.Meter {
	return m.meter
}

func (m *Metrics) commonAttrs(additional ...attribute.KeyValue) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("ref", m.ref),
	}

	return append(attrs, additional...)
}

func (m *Metrics) Start(name SpanName) Span {
	s := Span{
		m:     m,
		name:  name,
		start: time.Now(),
	}

	if m.meter != nil {
		m.pendingOperations.Add(context.Background(), 1, otelmetric.WithAttributes(
			m.commonAttrs(attribute.String("operation_type", name))...,
		))
	}

	return s
}

func (s Span) Finish(err error, attempts int) {
	if s.m.meter == nil {
		return
	}

	ctx := context.Background()

	status := OperationStatusSuccess
	if err != nil {
		status = OperationStatusFailure
	}

	attrs := s.m.commonAttrs(
		attribute.String("operation_type", s.name),
		attribute.String("operation_status", status),
	)

	s.m.latency.Record(time.Since(s.start).Microseconds(), attribute.NewSet(attrs...))

	s.m.operationsTotal.Add(ctx, 1, otelmetric.WithAttributes(attrs...))
	s.m.retryAttemptsTotal.Add(ctx, int64(attempts), otelmetric.WithAttributes(attrs...))
	s.m.retryAttempts.Record(ctx, int64(attempts), otelmetric.WithAttributes(attrs...))

	s.m.pendingOperations.Add(ctx, -1, otelmetric.WithAttributes(
		s.m.commonAttrs(attribute.String("operation_type", s.name))...,
	))

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.m.timeoutsTotal.Add(ctx, 1, otelmetric.WithAttributes(attrs...))
		}

		category, name := classifyError(err)
		errorAttrs := append(attrs,
			attribute.String("error_category", category),
			attribute.String("error_name", name),
		)

		s.m.errorsTotal.Add(ctx, 1, otelmetric.WithAttributes(errorAttrs...))
	}
}

func (m *Metrics) Push(ctx context.Context) error {
	if m.mp == nil {
		return nil
	}

	return m.mp.ForceFlush(ctx)
}

func (m *Metrics) Close(ctx context.Context) error {
	if m.mp == nil {
		return nil
	}

	return m.mp.Shutdown(ctx)
}
