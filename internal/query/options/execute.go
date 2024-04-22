package options

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	Syntax          Ydb_Query.Syntax
	ExecMode        Ydb_Query.ExecMode
	StatsMode       Ydb_Query.StatsMode
	GrpcOpts        []grpc.CallOption
	executeSettings struct {
		Syntax          Syntax
		Params          params.Parameters
		ExecMode        ExecMode
		StatsMode       StatsMode
		GrpcCallOptions []grpc.CallOption
		Trace           *trace.Query
		Allocator       *allocator.Allocator
	}
	Execute struct {
		executeSettings

		TxControl *tx.Control
	}
	ExecuteOption interface {
		ApplyExecuteOption(s *Execute)
	}
	txExecuteSettings struct {
		ExecuteOptions []ExecuteOption

		CommitTx bool
	}
	TxExecuteOption interface {
		ExecuteOption

		ApplyTxExecuteOption(s *txExecuteSettings)
	}
	txCommitOption   struct{}
	ParametersOption params.Parameters
	TxControlOption  struct {
		txControl *tx.Control
	}
)

func (t txCommitOption) ApplyExecuteOption(s *Execute) {
}

func (opt TxControlOption) ApplyExecuteOption(s *Execute) {
	s.TxControl = opt.txControl
}

func (t txCommitOption) ApplyTxExecuteOption(s *txExecuteSettings) {
	s.CommitTx = true
}

func (syntax Syntax) ApplyTxExecuteOption(s *txExecuteSettings) {
	s.ExecuteOptions = append(s.ExecuteOptions, syntax)
}

func (syntax Syntax) ApplyExecuteOption(s *Execute) {
	s.Syntax = syntax
}

const (
	SyntaxYQL        = Syntax(Ydb_Query.Syntax_SYNTAX_YQL_V1)
	SyntaxPostgreSQL = Syntax(Ydb_Query.Syntax_SYNTAX_PG)
)

func (params ParametersOption) ApplyTxExecuteOption(s *txExecuteSettings) {
	s.ExecuteOptions = append(s.ExecuteOptions, params)
}

func (params ParametersOption) ApplyExecuteOption(s *Execute) {
	s.Params = append(s.Params, params...)
}

func (opts GrpcOpts) ApplyExecuteOption(s *Execute) {
	s.GrpcCallOptions = append(s.GrpcCallOptions, opts...)
}

func (opts GrpcOpts) ApplyTxExecuteOption(s *txExecuteSettings) {
	s.ExecuteOptions = append(s.ExecuteOptions, opts)
}

func (mode StatsMode) ApplyTxExecuteOption(s *txExecuteSettings) {
	s.ExecuteOptions = append(s.ExecuteOptions, mode)
}

func (mode StatsMode) ApplyExecuteOption(s *Execute) {
	s.StatsMode = mode
}

func (mode ExecMode) ApplyTxExecuteOption(s *txExecuteSettings) {
	s.ExecuteOptions = append(s.ExecuteOptions, mode)
}

func (mode ExecMode) ApplyExecuteOption(s *Execute) {
	s.ExecMode = mode
}

const (
	ExecModeParse    = ExecMode(Ydb_Query.ExecMode_EXEC_MODE_PARSE)
	ExecModeValidate = ExecMode(Ydb_Query.ExecMode_EXEC_MODE_VALIDATE)
	ExecModeExplain  = ExecMode(Ydb_Query.ExecMode_EXEC_MODE_EXPLAIN)
	ExecModeExecute  = ExecMode(Ydb_Query.ExecMode_EXEC_MODE_EXECUTE)
)

const (
	StatsModeBasic   = StatsMode(Ydb_Query.StatsMode_STATS_MODE_BASIC)
	StatsModeNone    = StatsMode(Ydb_Query.StatsMode_STATS_MODE_NONE)
	StatsModeFull    = StatsMode(Ydb_Query.StatsMode_STATS_MODE_FULL)
	StatsModeProfile = StatsMode(Ydb_Query.StatsMode_STATS_MODE_PROFILE)
)

func defaultExecuteSettings() executeSettings {
	return executeSettings{
		Syntax:    SyntaxYQL,
		ExecMode:  ExecModeExecute,
		StatsMode: StatsModeNone,
		Trace:     &trace.Query{},
	}
}

func ExecuteSettings[T ExecuteOption](opts ...T) (settings *Execute) {
	settings = &Execute{
		executeSettings: defaultExecuteSettings(),
	}
	settings.executeSettings = defaultExecuteSettings()
	settings.TxControl = tx.DefaultTxControl()
	for _, opt := range opts {
		opt.ApplyExecuteOption(settings)
	}

	return settings
}

func TxExecuteSettings(id string, opts ...TxExecuteOption) (settings *txExecuteSettings) {
	settings = &txExecuteSettings{
		ExecuteOptions: []ExecuteOption{
			WithTxControl(tx.NewControl(tx.WithTxID(id))),
		},
	}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyTxExecuteOption(settings)
		}
	}

	return settings
}

var _ ExecuteOption = ParametersOption{}

func WithParameters(parameters *params.Parameters) ParametersOption {
	return ParametersOption(*parameters)
}

var (
	_ ExecuteOption   = ExecMode(0)
	_ ExecuteOption   = StatsMode(0)
	_ TxExecuteOption = ExecMode(0)
	_ TxExecuteOption = StatsMode(0)
	_ TxExecuteOption = txCommitOption{}
	_ ExecuteOption   = TxControlOption{}
	_ TxExecuteOption = traceOption{}
	_ ExecuteOption   = traceOption{}
)

func WithCommit() txCommitOption {
	return txCommitOption{}
}

type ExecModeOption = ExecMode

func WithExecMode(mode ExecMode) ExecMode {
	return mode
}

type SyntaxOption = Syntax

func WithSyntax(syntax Syntax) SyntaxOption {
	return syntax
}

type StatsModeOption = StatsMode

func WithStatsMode(mode StatsMode) StatsMode {
	return mode
}

func WithCallOptions(opts ...grpc.CallOption) GrpcOpts {
	return opts
}

func WithTxControl(txControl *tx.Control) TxControlOption {
	return TxControlOption{txControl}
}
