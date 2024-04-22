package options

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ DoOption = idempotentOption{}
	_ DoOption = labelOption("")
	_ DoOption = traceOption{}

	_ DoTxOption = idempotentOption{}
	_ DoTxOption = labelOption("")
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
		doOpts     []DoOption
		txSettings tx.Settings
	}

	idempotentOption   struct{}
	labelOption        string
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

func (opt idempotentOption) applyDoTxOption(s *doTxSettings) {
	s.doOpts = append(s.doOpts, opt)
}

func (idempotentOption) applyDoOption(s *doSettings) {
	s.retryOpts = append(s.retryOpts, retry.WithIdempotent(true))
}

func (opt labelOption) applyDoOption(s *doSettings) {
	s.retryOpts = append(s.retryOpts, retry.WithLabel(string(opt)))
}

func (opt labelOption) applyDoTxOption(s *doTxSettings) {
	s.doOpts = append(s.doOpts, opt)
}

func (opt doTxSettingsOption) applyDoTxOption(opts *doTxSettings) {
	opts.txSettings = opt.txSettings
}

func WithTxSettings(txSettings tx.Settings) doTxSettingsOption {
	return doTxSettingsOption{txSettings: txSettings}
}

func WithIdempotent() idempotentOption {
	return idempotentOption{}
}

func WithLabel(lbl string) labelOption {
	return labelOption(lbl)
}

func WithTrace(t *trace.Query) traceOption {
	return traceOption{t: t}
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
