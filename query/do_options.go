package query

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ DoOption = idempotentOption{}
	_ DoOption = traceOption{}
)

func (idempotentOption) applyDoOption(opts *DoOptions) {
	opts.Idempotent = true
	opts.RetryOptions = append(opts.RetryOptions, retry.WithIdempotent(true))
}

func (opt traceOption) applyDoOption(o *DoOptions) {
	o.Trace = o.Trace.Compose(opt.t)
}

func NewDoOptions(opts ...DoOption) (doOptions DoOptions) {
	doOptions.Trace = &trace.Query{}

	for _, opt := range opts {
		opt.applyDoOption(&doOptions)
	}

	return doOptions
}
