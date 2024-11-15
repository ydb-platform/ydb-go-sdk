package query

import (
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
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

// WithResponsePartLimitSizeBytes limit size of each part (data portion) in stream for query service resoponse
// it isn't limit total size of answer
func WithResponsePartLimitSizeBytes(size int64) options.Execute {
	return options.WithResponsePartLimitSizeBytes(size)
}

func WithCallOptions(opts ...grpc.CallOption) options.Execute {
	return options.WithCallOptions(opts...)
}

// WithResourcePool is an option for define resource pool for execute query
//
// Read more https://ydb.tech/docs/ru/dev/resource-consumption-management
func WithResourcePool(id string) options.Execute {
	return options.WithResourcePool(id)
}
