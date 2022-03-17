package table

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Event declares event type
//
// Warning: This is an experimental feature and could change at any time
type Event uint8

const (
	// EventIntermediate declares intermediate event type
	//
	// Warning: This is an experimental feature and could change at any time
	EventIntermediate = Event(iota)

	// EventDone declares done event type
	//
	// Warning: This is an experimental feature and could change at any time
	EventDone
)

type Options struct {
	Idempotent      bool
	TxSettings      *TransactionSettings
	TxCommitOptions []options.CommitTransactionOption
	FastBackoff     retry.Backoff
	SlowBackoff     retry.Backoff
	Trace           trace.Table

	// IsTraceError checks err before submit trace
	//
	// Warning: This is an experimental feature and could change at any time
	IsTraceError func(event Event, err error) bool
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
