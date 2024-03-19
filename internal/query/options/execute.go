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
	Execute struct {
		commonExecuteSettings

		txControl *tx.Control
	}
	ExecuteOption interface {
		applyExecuteOption(s *Execute)
	}
	txExecuteSettings struct {
		ExecuteSettings *Execute

		commitTx bool
	}
	TxExecuteOption interface {
		applyTxExecuteOption(s *txExecuteSettings)
	}
	txCommitOption   struct{}
	ParametersOption params.Parameters
	TxControlOption  struct {
		txControl *tx.Control
	}
)

func (opt TxControlOption) applyExecuteOption(s *Execute) {
	s.txControl = opt.txControl
}

func (t txCommitOption) applyTxExecuteOption(s *txExecuteSettings) {
	s.commitTx = true
}

func (syntax Syntax) applyTxExecuteOption(s *txExecuteSettings) {
	syntax.applyExecuteOption(s.ExecuteSettings)
}

func (syntax Syntax) applyExecuteOption(s *Execute) {
	s.syntax = syntax
}

const (
	SyntaxYQL        = Syntax(Ydb_Query.Syntax_SYNTAX_YQL_V1)
	SyntaxPostgreSQL = Syntax(Ydb_Query.Syntax_SYNTAX_PG)
)

func (params ParametersOption) applyTxExecuteOption(s *txExecuteSettings) {
	params.applyExecuteOption(s.ExecuteSettings)
}

func (params ParametersOption) applyExecuteOption(s *Execute) {
	s.params = append(s.params, params...)
}

func (opts CallOptions) applyExecuteOption(s *Execute) {
	s.callOptions = append(s.callOptions, opts...)
}

func (opts CallOptions) applyTxExecuteOption(s *txExecuteSettings) {
	opts.applyExecuteOption(s.ExecuteSettings)
}

func (mode StatsMode) applyTxExecuteOption(s *txExecuteSettings) {
	mode.applyExecuteOption(s.ExecuteSettings)
}

func (mode StatsMode) applyExecuteOption(s *Execute) {
	s.statsMode = mode
}

func (mode ExecMode) applyTxExecuteOption(s *txExecuteSettings) {
	mode.applyExecuteOption(s.ExecuteSettings)
}

func (mode ExecMode) applyExecuteOption(s *Execute) {
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

func ExecuteSettings(opts ...ExecuteOption) (settings *Execute) {
	settings = &Execute{
		commonExecuteSettings: defaultCommonExecuteSettings(),
	}
	settings.commonExecuteSettings = defaultCommonExecuteSettings()
	settings.txControl = tx.DefaultTxControl()
	for _, opt := range opts {
		if opt != nil {
			opt.applyExecuteOption(settings)
		}
	}

	return settings
}

func (s *Execute) TxControl() *tx.Control {
	return s.txControl
}

func (s *Execute) SetTxControl(ctrl *tx.Control) {
	s.txControl = ctrl
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

func TxExecuteSettings(id string, opts ...TxExecuteOption) (settings *txExecuteSettings) {
	settings = &txExecuteSettings{
		ExecuteSettings: ExecuteSettings(WithTxControl(tx.NewControl(tx.WithTxID(id)))),
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyTxExecuteOption(settings)
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

func WithTxControl(txControl *tx.Control) TxControlOption {
	return TxControlOption{txControl}
}
