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
		NodeID() uint32
		Status() string
	}
	// Executor defines main operations
	//
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	Executor interface {
		// Exec executes query.
		//
		// Exec used by default:
		// - DefaultTxControl
		Exec(ctx context.Context, query string, opts ...options.Execute) error

		// Query executes query.
		//
		// Query used by default:
		// - DefaultTxControl
		Query(ctx context.Context, query string, opts ...options.Execute) (r Result, err error)
	}
	Session interface {
		SessionInfo
		Executor

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

func WithParameters(parameters *params.Parameters) options.ParametersOption {
	return options.WithParameters(parameters)
}

func WithTxControl(txControl *tx.Control) options.Execute {
	return options.WithTxControl(txControl)
}

func WithTxSettings(txSettings tx.Settings) options.DoTxOption {
	return options.WithTxSettings(txSettings)
}

func WithCommit() options.Execute {
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
