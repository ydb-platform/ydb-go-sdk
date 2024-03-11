package query

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

type idempotentOption struct{}

func WithIdempotent() idempotentOption {
	return idempotentOption{}
}

type traceOption struct {
	t *trace.Query
}

func WithTrace(t trace.Query) traceOption {
	return traceOption{t: &t}
}
