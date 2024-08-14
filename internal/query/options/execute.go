package options

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
)

type (
	Syntax                Ydb_Query.Syntax
	ExecMode              Ydb_Query.ExecMode
	StatsMode             Ydb_Query.StatsMode
	CallOptions           []grpc.CallOption
	commonExecuteSettings struct {
		syntax      Syntax
		params      params.Parameters
		execMode    ExecMode
		statsMode   StatsMode
		callOptions []grpc.CallOption
	}
	executeSettings struct {
		commonExecuteSettings

		txControl *tx.Control
	}
	Execute interface {
		applyExecuteOption(s *executeSettings)
	}
	txCommitOption   struct{}
	ParametersOption params.Parameters
	txControlOption  tx.Control
)

func (t txCommitOption) applyExecuteOption(s *executeSettings) {
	s.txControl.Commit = true
}

func (txControl txControlOption) applyExecuteOption(s *executeSettings) {
	s.txControl = (*tx.Control)(&txControl)
}

func (syntax Syntax) applyExecuteOption(s *executeSettings) {
	s.syntax = syntax
}

const (
	SyntaxYQL        = Syntax(Ydb_Query.Syntax_SYNTAX_YQL_V1)
	SyntaxPostgreSQL = Syntax(Ydb_Query.Syntax_SYNTAX_PG)
)

func (params ParametersOption) applyExecuteOption(s *executeSettings) {
	s.params = append(s.params, params...)
}

func (opts CallOptions) applyExecuteOption(s *executeSettings) {
	s.callOptions = append(s.callOptions, opts...)
}

func (mode StatsMode) applyExecuteOption(s *executeSettings) {
	s.statsMode = mode
}

func (mode ExecMode) applyExecuteOption(s *executeSettings) {
	s.execMode = mode
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

func defaultCommonExecuteSettings() commonExecuteSettings {
	return commonExecuteSettings{
		syntax:    SyntaxYQL,
		execMode:  ExecModeExecute,
		statsMode: StatsModeNone,
	}
}

func ExecuteSettings(opts ...Execute) *executeSettings {
	settings := &executeSettings{
		commonExecuteSettings: defaultCommonExecuteSettings(),
		txControl:             tx.DefaultTxControl(),
	}

	for _, opt := range opts {
		if opt != nil {
			opt.applyExecuteOption(settings)
		}
	}

	return settings
}

func (s *executeSettings) TxControl() *tx.Control {
	return s.txControl
}

func (s *commonExecuteSettings) CallOptions() []grpc.CallOption {
	return s.callOptions
}

func (s *commonExecuteSettings) Syntax() Syntax {
	return s.syntax
}

func (s *commonExecuteSettings) ExecMode() ExecMode {
	return s.execMode
}

func (s *commonExecuteSettings) StatsMode() StatsMode {
	return s.statsMode
}

func (s *commonExecuteSettings) Params() *params.Parameters {
	if len(s.params) == 0 {
		return nil
	}

	return &s.params
}

var _ Execute = ParametersOption{}

func WithParameters(parameters *params.Parameters) ParametersOption {
	return ParametersOption(*parameters)
}

var (
	_ Execute = ExecMode(0)
	_ Execute = StatsMode(0)
	_ Execute = ExecMode(0)
	_ Execute = StatsMode(0)
	_ Execute = txCommitOption{}
	_ Execute = txControlOption{}
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

func WithCallOptions(opts ...grpc.CallOption) CallOptions {
	return opts
}

func WithTxControl(txControl *tx.Control) *txControlOption {
	return (*txControlOption)(txControl)
}
