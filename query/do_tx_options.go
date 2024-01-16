package query

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

var _ DoTxOption = idempotentOption{}

func (idempotentOption) applyDoTxOption(opts *DoTxOptions) {
	opts.Idempotent = true
	opts.RetryOptions = append(opts.RetryOptions, retry.WithIdempotent(true))
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

func NewDoTxOptions(opts ...DoTxOption) (doTxOptions DoTxOptions) {
	doTxOptions.TxSettings = TxSettings(WithDefaultTxMode())
	for _, opt := range opts {
		opt.applyDoTxOption(&doTxOptions)
	}
	return doTxOptions
}
