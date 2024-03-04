package query

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ DoTxOption = idempotentOption{}
	_ DoTxOption = traceOption{}
)

func (idempotentOption) applyDoTxOption(opts *DoTxOptions) {
	opts.Idempotent = true
	opts.RetryOptions = append(opts.RetryOptions, retry.WithIdempotent(true))
}

func (opt traceOption) applyDoTxOption(o *DoTxOptions) {
	o.Trace = o.Trace.Compose(opt.t)
}

var _ DoTxOption = doTxSettingsOption{}

type doTxSettingsOption struct {
	txSettings TransactionSettings
}

func (opt doTxSettingsOption) applyDoTxOption(opts *DoTxOptions) {
	opts.TxSettings = opt.txSettings
}

func WithTxSettings(txSettings TransactionSettings) doTxSettingsOption {
	return doTxSettingsOption{txSettings: txSettings}
}

func NewDoTxOptions(opts ...DoTxOption) DoTxOptions {
	doTxOptions := DoTxOptions{}
	doTxOptions.TxSettings = TxSettings(WithDefaultTxMode())
	doTxOptions.Trace = &trace.Query{}

	for _, opt := range opts {
		opt.applyDoTxOption(&doTxOptions)
	}

	return doTxOptions
}
