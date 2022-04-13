package table

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Options struct {
	Idempotent      bool
	TxSettings      *TransactionSettings
	TxCommitOptions []options.CommitTransactionOption
	FastBackoff     backoff.Backoff
	SlowBackoff     backoff.Backoff
	Trace           trace.Table
}

type Option func(o *Options)

func WithIdempotent() Option {
	return func(o *Options) {
		o.Idempotent = true
	}
}

func WithTxSettings(tx *TransactionSettings) Option {
	return func(o *Options) {
		o.TxSettings = tx
	}
}

func WithTxCommitOptions(opts ...options.CommitTransactionOption) Option {
	return func(o *Options) {
		o.TxCommitOptions = append(o.TxCommitOptions, opts...)
	}
}

func WithTrace(t trace.Table) Option {
	return func(o *Options) {
		o.Trace = o.Trace.Compose(t)
	}
}
