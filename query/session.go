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
		// - DefaultTxControl
		// - flag WithKeepInCache(true) if params is not empty.
		Execute(ctx context.Context, query string, opts ...options.ExecuteOption) (tx Transaction, r Result, err error)

		Begin(ctx context.Context, txSettings TransactionSettings) (Transaction, error)
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

func WithParameters(parameters *params.Parameters) options.ExecuteOption {
	return options.WithParameters(parameters)
}

func WithTxControl(txControl *tx.Control) options.ExecuteOption {
	return options.WithTxControl(txControl)
}

func WithTxSettings(txSettings tx.Settings) options.DoTxOption {
	return options.WithTxSettings(txSettings)
}

func WithCommit() options.TxExecuteOption {
	return options.WithCommit()
}

func WithExecMode(mode options.ExecMode) options.ExecuteOption {
	return options.WithExecMode(mode)
}

func WithSyntax(syntax options.Syntax) options.ExecuteOption {
	return options.WithSyntax(syntax)
}

func WithStatsMode(mode options.StatsMode) options.ExecuteOption {
	return options.WithStatsMode(mode)
}

func WithCallOptions(opts ...grpc.CallOption) options.ExecuteOption {
	return options.WithCallOptions(opts...)
}
