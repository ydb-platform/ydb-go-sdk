package query

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

var (
	_ DoOption   = idempotentOption{}
	_ DoTxOption = idempotentOption{}
)

func (idempotentOption) applyDoOption(opts *DoOptions) {
	opts.Idempotent = true
	opts.RetryOptions = append(opts.RetryOptions, retry.WithIdempotent(true))
}

func NewDoOptions(opts ...DoOption) (doOptions DoOptions) {
	for _, opt := range opts {
		opt.applyDoOption(&doOptions)
	}
	return doOptions
}
