package options

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ DoOption = retryOptionsOption(nil)
	_ DoOption = traceOption{t: nil}

	_ DoTxOption = retryOptionsOption(nil)
	_ DoTxOption = traceOption{t: nil}
	_ DoTxOption = doTxSettingsOption{txSettings: nil}
)

type (
	DoOption interface {
		applyDoOption(s *doSettings)
	}

	doSettings struct {
		retryOpts []retry.Option
		trace     *trace.Query
	}

	DoTxOption interface {
		applyDoTxOption(o *doTxSettings)
	}

	doTxSettings struct {
		doOpts     []DoOption
		txSettings tx.Settings
	}

	retryOptionsOption []retry.Option
	traceOption        struct {
		t *trace.Query
	}
	doTxSettingsOption struct {
		txSettings tx.Settings
	}
)

func (s *doSettings) Trace() *trace.Query {
	return s.trace
}

func (s *doSettings) RetryOpts() []retry.Option {
	return s.retryOpts
}

func (s *doTxSettings) DoOpts() []DoOption {
	return s.doOpts
}

func (s *doTxSettings) TxSettings() tx.Settings {
	return s.txSettings
}

func (opt traceOption) applyDoOption(s *doSettings) {
	s.trace = s.trace.Compose(opt.t)
}

func (opt traceOption) applyDoTxOption(s *doTxSettings) {
	s.doOpts = append(s.doOpts, opt)
}

func (opts retryOptionsOption) applyDoOption(s *doSettings) {
	s.retryOpts = append(s.retryOpts, opts...)
}

func (opts retryOptionsOption) applyDoTxOption(s *doTxSettings) {
	s.doOpts = append(s.doOpts, opts)
}

func (opt doTxSettingsOption) applyDoTxOption(opts *doTxSettings) {
	opts.txSettings = opt.txSettings
}

func WithTxSettings(txSettings tx.Settings) doTxSettingsOption {
	return doTxSettingsOption{txSettings: txSettings}
}

func WithIdempotent() retryOptionsOption {
	return []retry.Option{retry.WithIdempotent(true)}
}

func WithLabel(lbl string) retryOptionsOption {
	return []retry.Option{retry.WithLabel(lbl)}
}

func WithTrace(t *trace.Query) traceOption {
	return traceOption{t: t}
}

func WithRetryBudget(b budget.Budget) retryOptionsOption {
	return []retry.Option{retry.WithBudget(b)}
}

func ParseDoOpts(t *trace.Query, opts ...DoOption) (s *doSettings) {
	s = &doSettings{
		retryOpts: nil,
		trace:     t,
	}

	for _, opt := range opts {
		if opt != nil {
			opt.applyDoOption(s)
		}
	}

	return s
}

func ParseDoTxOpts(t *trace.Query, opts ...DoTxOption) (s *doTxSettings) {
	s = &doTxSettings{
		txSettings: tx.NewSettings(tx.WithDefaultTxMode()),
		doOpts: []DoOption{
			WithTrace(t),
		},
	}

	for _, opt := range opts {
		if opt != nil {
			opt.applyDoTxOption(s)
		}
	}

	return s
}
