package options

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

type traceOption struct {
	t *trace.Query
}

func (opt traceOption) ApplyTxExecuteOption(s *txExecuteSettings) {
	s.ExecuteOptions = append(s.ExecuteOptions, opt)
}

func (opt traceOption) ApplyExecuteOption(s *Execute) {
	s.Trace = s.Trace.Compose(opt.t)
}

func (opt traceOption) applyDoOption(s *doSettings) {
	s.trace = s.trace.Compose(opt.t)
}

func (opt traceOption) applyDoTxOption(s *doTxSettings) {
	s.doOpts = append(s.doOpts, opt)
}
