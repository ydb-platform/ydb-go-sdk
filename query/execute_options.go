package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
)

type (
	Syntax                Ydb_Query.Syntax
	ExecMode              Ydb_Query.ExecMode
	StatsMode             Ydb_Query.StatsMode
	callOptions           []grpc.CallOption
	commonExecuteSettings struct {
		syntax      Syntax
		params      params.Parameters
		execMode    ExecMode
		statsMode   StatsMode
		callOptions []grpc.CallOption
	}
	executeSettings struct {
		commonExecuteSettings

		txControl *TransactionControl
	}
	ExecuteOption interface {
		applyExecuteOption(s *executeSettings)
	}
	txExecuteSettings struct {
		commonExecuteSettings
	}
	TxExecuteOption interface {
		applyTxExecuteOption(s *txExecuteSettings)
	}
	parametersOption params.Parameters
)

func (syntax Syntax) applyTxExecuteOption(s *txExecuteSettings) {
	s.syntax = syntax
}

func (syntax Syntax) applyExecuteOption(s *executeSettings) {
	s.syntax = syntax
}

const (
	SyntaxYQL        = Syntax(Ydb_Query.Syntax_SYNTAX_YQL_V1)
	SyntaxPostgreSQL = Syntax(Ydb_Query.Syntax_SYNTAX_PG)
)

func (params parametersOption) applyTxExecuteOption(s *txExecuteSettings) {
	s.params = append(s.params, params...)
}

func (params parametersOption) applyExecuteOption(s *executeSettings) {
	s.params = append(s.params, params...)
}

func (opts callOptions) applyExecuteOption(s *executeSettings) {
	s.callOptions = append(s.callOptions, opts...)
}

func (opts callOptions) applyTxExecuteOption(s *txExecuteSettings) {
	s.callOptions = append(s.callOptions, opts...)
}

func (mode StatsMode) applyTxExecuteOption(s *txExecuteSettings) {
	s.statsMode = mode
}

func (mode StatsMode) applyExecuteOption(s *executeSettings) {
	s.statsMode = mode
}

func (mode ExecMode) applyTxExecuteOption(s *txExecuteSettings) {
	s.execMode = mode
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

func ExecuteSettings(opts ...ExecuteOption) *executeSettings {
	settings := &executeSettings{
		commonExecuteSettings: defaultCommonExecuteSettings(),
	}
	settings.commonExecuteSettings = defaultCommonExecuteSettings()
	settings.txControl = DefaultTxControl()
	for _, opt := range opts {
		opt.applyExecuteOption(settings)
	}

	return settings
}

func (s *executeSettings) TxControl() *TransactionControl {
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

func TxExecuteSettings(opts ...TxExecuteOption) *txExecuteSettings {
	settings := &txExecuteSettings{
		commonExecuteSettings: defaultCommonExecuteSettings(),
	}
	for _, opt := range opts {
		opt.applyTxExecuteOption(settings)
	}

	return settings
}

var _ ExecuteOption = (*parametersOption)(nil)

func WithParameters(parameters *params.Parameters) *parametersOption {
	params := parametersOption(*parameters)

	return &params
}

var (
	_ ExecuteOption   = ExecMode(0)
	_ ExecuteOption   = StatsMode(0)
	_ TxExecuteOption = ExecMode(0)
	_ TxExecuteOption = StatsMode(0)
)

func WithExecMode(mode ExecMode) ExecMode {
	return mode
}

func WithSyntax(syntax Syntax) Syntax {
	return syntax
}

func WithStatsMode(mode StatsMode) StatsMode {
	return mode
}

func WithCallOptions(opts ...grpc.CallOption) callOptions {
	return opts
}

func WithTxControl(txControl *TransactionControl) *transactionControlOption {
	return &transactionControlOption{
		txControl: txControl,
	}
}
