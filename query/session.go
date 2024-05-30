package query

import (
	"context"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
)

type (
	SessionInfo interface {
		ID() string
		NodeID() int64
		Status() string
	}

	Session interface {
		SessionInfo

		// Execute executes query.
		//
		// Execute used by default:
		// - DefaultTxControl (NoTx)
		// - flag WithKeepInCache(true) if params is not empty.
		Execute(ctx context.Context, query string, opts ...options.ExecuteOption) (tx Transaction, r Result, err error)

		Begin(ctx context.Context, txSettings TransactionSettings) (Transaction, error)

		// ReadRow is a helper which read only one row from first result set in result
		//
		// ReadRow returns error if result contains more than one result set or more than one row
		//
		// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
		ReadRow(ctx context.Context, query string, opts ...options.ExecuteOption) (Row, error)

		// ReadResultSet is a helper which read all rows from first result set in result
		//
		// ReadRow returns error if result contains more than one result set
		//
		// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
		ReadResultSet(ctx context.Context, query string, opts ...options.ExecuteOption) (ResultSet, error)
	}
)

const (
	SyntaxYQL        = options.SyntaxYQL
	SyntaxPostgreSQL = options.SyntaxPostgreSQL
)

const (
	ExecModeParse    = options.ExecModeParse
	ExecModeValidate = options.ExecModeValidate
	ExecModeExplain  = options.ExecModeExplain
	ExecModeExecute  = options.ExecModeExecute
)

const (
	StatsModeBasic   = options.StatsModeBasic
	StatsModeNone    = options.StatsModeNone
	StatsModeFull    = options.StatsModeFull
	StatsModeProfile = options.StatsModeProfile
)

func WithParameters(parameters *params.Parameters) options.ParametersOption {
	return options.WithParameters(parameters)
}

func WithTxControl(txControl *tx.Control) options.TxControlOption {
	return options.WithTxControl(txControl)
}

func WithTxSettings(txSettings tx.Settings) options.DoTxOption {
	return options.WithTxSettings(txSettings)
}

func WithCommit() options.TxExecuteOption {
	return options.WithCommit()
}

func WithExecMode(mode options.ExecMode) options.ExecModeOption {
	return options.WithExecMode(mode)
}

func WithSyntax(syntax options.Syntax) options.SyntaxOption {
	return options.WithSyntax(syntax)
}

func WithStatsMode(mode options.StatsMode) options.StatsModeOption {
	return options.WithStatsMode(mode)
}

func WithCallOptions(opts ...grpc.CallOption) options.CallOptions {
	return options.WithCallOptions(opts...)
}
