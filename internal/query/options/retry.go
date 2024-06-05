package options

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ DoOption = retryOptionsOption(nil)
	_ DoOption = traceOption{}

	_ DoTxOption = retryOptionsOption(nil)
	_ DoTxOption = traceOption{}
	_ DoTxOption = doTxSettingsOption{}
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
		doSettings
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

func (s *doTxSettings) TxSettings() tx.Settings {
	return s.txSettings
}

func (opt traceOption) applyDoOption(s *doSettings) {
	s.trace = s.trace.Compose(opt.t)
}

func (opt traceOption) applyDoTxOption(s *doTxSettings) {
	opt.applyDoOption(&s.doSettings)
}

func (opts retryOptionsOption) applyDoOption(s *doSettings) {
	s.retryOpts = append(s.retryOpts, opts...)
}

func (opts retryOptionsOption) applyDoTxOption(s *doTxSettings) {
	opts.applyDoOption(&s.doSettings)
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
		trace: t,
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
		doSettings: doSettings{
			trace: t,
		},
	}

	for _, opt := range opts {
		if opt != nil {
			opt.applyDoTxOption(s)
		}
	}

	return s
}
