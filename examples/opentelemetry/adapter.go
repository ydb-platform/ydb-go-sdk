package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
	ydbtrace "github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// otelAdapter is a spans.Adapter implementation backed by the OpenTelemetry
// Go SDK. It maps span names emitted by ydb-go-sdk to the appropriate
// trace.SpanKind and translates ydb-go-sdk KeyValue typed attributes to
// otel/attribute.KeyValue.
//
// In addition to whatever attributes are passed to Start(), the adapter
// attaches the following resource-equivalent attributes to every span it
// creates:
//
//	db.system.name = "ydb"
//	db.namespace   = config.Database
//	server.address = host part of config.Endpoint
//	server.port    = port part of config.Endpoint
type otelAdapter struct {
	tracer    trace.Tracer
	staticAtt []attribute.KeyValue
}

func newOTelAdapter(tracerName, database, endpoint string) *otelAdapter {
	host, portStr, err := net.SplitHostPort(strings.TrimPrefix(strings.TrimPrefix(endpoint, "grpcs://"), "grpc://"))
	if err != nil {
		host, portStr = endpoint, ""
	}
	staticAtt := []attribute.KeyValue{
		attribute.String(spans.AttrDBSystemName, "ydb"),
		attribute.String(spans.AttrDBNamespace, database),
		attribute.String(spans.AttrServerAddress, host),
	}
	if port, err := strconv.Atoi(portStr); err == nil {
		staticAtt = append(staticAtt, attribute.Int(spans.AttrServerPort, port))
	}

	return &otelAdapter{
		tracer:    otel.Tracer(tracerName),
		staticAtt: staticAtt,
	}
}

// Details enables every event category emitted by ydb-go-sdk; for finer
// control return only the bits you actually want to translate.
func (*otelAdapter) Details() ydbtrace.Details {
	return ydbtrace.DetailsAll
}

// SpanFromContext returns a wrapper over the span currently associated with
// the context, so log/error calls can be attributed to the parent span when
// no new span is started.
func (a *otelAdapter) SpanFromContext(ctx context.Context) spans.Span {
	return &otelSpan{span: trace.SpanFromContext(ctx)}
}

// Start creates a new span. The kind is selected by name, see kindFor below.
func (a *otelAdapter) Start(
	ctx context.Context, name string, attrs ...spans.KeyValue,
) (context.Context, spans.Span) {
	otelAttrs := append([]attribute.KeyValue{}, a.staticAtt...)
	otelAttrs = append(otelAttrs, toOtelAttributes(attrs)...)

	ctx, span := a.tracer.Start(ctx, name,
		trace.WithSpanKind(kindFor(name)),
		trace.WithAttributes(otelAttrs...),
	)

	return ctx, &otelSpan{span: span}
}

// kindFor selects the OpenTelemetry SpanKind for a span emitted by
// ydb-go-sdk's spans package. CLIENT for actual gRPC calls to the YDB
// QueryService, INTERNAL for the retry-loop bookkeeping spans.
func kindFor(name string) trace.SpanKind {
	switch name {
	case spans.SpanNameCreateSession,
		spans.SpanNameExecuteQuery,
		spans.SpanNameBeginTransaction,
		spans.SpanNameCommit,
		spans.SpanNameRollback:
		return trace.SpanKindClient
	case spans.SpanNameRunWithRetry, spans.SpanNameTry, spans.SpanNameDriverInitialize:
		return trace.SpanKindInternal
	default:
		return trace.SpanKindInternal
	}
}

// otelSpan adapts an OpenTelemetry trace.Span to spans.Span.
type otelSpan struct {
	span trace.Span
}

func (s *otelSpan) ID() (string, bool) {
	id := s.span.SpanContext().SpanID()
	if !id.IsValid() {
		return "", false
	}

	return id.String(), true
}

func (s *otelSpan) TraceID() (string, bool) {
	id := s.span.SpanContext().TraceID()
	if !id.IsValid() {
		return "", false
	}

	return id.String(), true
}

func (s *otelSpan) SetAttributes(attrs ...spans.KeyValue) {
	if otelAttrs := toOtelAttributes(attrs); len(otelAttrs) > 0 {
		s.span.SetAttributes(otelAttrs...)
	}
}

func (s *otelSpan) Link(other spans.Span, attrs ...spans.KeyValue) {
	if other == nil {
		return
	}
	o, ok := other.(*otelSpan)
	if !ok {
		return
	}
	s.span.AddLink(trace.Link{
		SpanContext: o.span.SpanContext(),
		Attributes:  toOtelAttributes(attrs),
	})
}

func (s *otelSpan) Log(msg string, attrs ...spans.KeyValue) {
	s.span.AddEvent(msg, trace.WithAttributes(toOtelAttributes(attrs)...))
}

func (s *otelSpan) Warn(err error, attrs ...spans.KeyValue) {
	s.span.RecordError(err, trace.WithAttributes(toOtelAttributes(attrs)...))
}

func (s *otelSpan) Error(err error, attrs ...spans.KeyValue) {
	s.span.RecordError(err, trace.WithAttributes(toOtelAttributes(attrs)...))
	s.span.SetStatus(codes.Error, errString(err))
	for _, kv := range toOtelAttributes(attrs) {
		s.span.SetAttributes(kv)
	}
}

func (s *otelSpan) End(attrs ...spans.KeyValue) {
	if otelAttrs := toOtelAttributes(attrs); len(otelAttrs) > 0 {
		s.span.SetAttributes(otelAttrs...)
	}
	s.span.End()
}

func errString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

func toOtelAttributes(in []spans.KeyValue) []attribute.KeyValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(in))
	for _, kvf := range in {
		out = append(out, toOtelAttribute(kvf))
	}

	return out
}

// toOtelAttribute converts a ydb-go-sdk KeyValue to an OTel attribute. We
// switch on the dynamic value type returned by AnyValue() instead of the
// internal FieldType enum so we don't depend on internal/kv.
func toOtelAttribute(kvf spans.KeyValue) attribute.KeyValue {
	switch v := kvf.AnyValue().(type) {
	case int:
		return attribute.Int(kvf.Key(), v)
	case int64:
		return attribute.Int64(kvf.Key(), v)
	case string:
		return attribute.String(kvf.Key(), v)
	case bool:
		return attribute.Bool(kvf.Key(), v)
	case time.Duration:
		return attribute.String(kvf.Key(), v.String())
	case []string:
		return attribute.StringSlice(kvf.Key(), v)
	case error:
		return attribute.String(kvf.Key(), errString(v))
	case fmt.Stringer:
		if v == nil {
			return attribute.String(kvf.Key(), "")
		}

		return attribute.String(kvf.Key(), v.String())
	default:
		return attribute.String(kvf.Key(), fmt.Sprint(v))
	}
}
