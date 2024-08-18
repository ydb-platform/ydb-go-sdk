package query

import (
	"context"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

type (
	SessionInfo interface {
		ID() string
		NodeID() uint32
		Status() string
	}
	Session interface {
		SessionInfo
		Executor

		Begin(ctx context.Context, txSettings TransactionSettings) (Transaction, error)
	}
	Stats = stats.QueryStats
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

func WithParameters(parameters *params.Parameters) options.Execute {
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

func WithExecMode(mode options.ExecMode) options.Execute {
	return options.WithExecMode(mode)
}

func WithSyntax(syntax options.Syntax) options.Execute {
	return options.WithSyntax(syntax)
}

func WithStatsMode(mode options.StatsMode, callback func(Stats)) options.Execute {
	return options.WithStatsMode(mode, callback)
}

func WithCallOptions(opts ...grpc.CallOption) options.Execute {
	return options.WithCallOptions(opts...)
}
