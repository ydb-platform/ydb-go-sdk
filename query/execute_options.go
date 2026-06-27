package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
)

type ExecuteOption = options.Execute

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

func WithParameters(parameters params.Parameters) ExecuteOption {
	return options.WithParameters(parameters)
}

func WithTxControl(txControl *tx.Control) ExecuteOption {
	return options.WithTxControl(txControl)
}

func WithTxSettings(txSettings tx.Settings) options.DoTxOption {
	return options.WithTxSettings(txSettings)
}

func WithCommit() ExecuteOption {
	return options.WithCommit()
}

func WithExecMode(mode options.ExecMode) ExecuteOption {
	return options.WithExecMode(mode)
}

func WithSyntax(syntax options.Syntax) ExecuteOption {
	return options.WithSyntax(syntax)
}

func WithStatsMode(mode options.StatsMode, callback func(Stats)) ExecuteOption {
	return options.WithStatsMode(mode, callback)
}

// WithIssuesHandler is the option which helps collect issues generated during query execution
// May be more than one call of callback during query execution
func WithIssuesHandler(callback func(issues []*Ydb_Issue.IssueMessage)) ExecuteOption {
	return options.WithIssuesHandler(callback)
}

// WithResponsePartLimitSizeBytes limit size of each part (data portion) in stream for query service resoponse
// it isn't limit total size of answer
func WithResponsePartLimitSizeBytes(size int64) ExecuteOption {
	return options.WithResponsePartLimitSizeBytes(size)
}

// WithResponsePartPrefetch configures how many ExecuteQuery response parts the
// client reads ahead of application consumption on streamed response-part
// readers that support prefetching. Values above zero enable a small buffer
// and a background reader so gRPC Recv can overlap with CPU work between
// reads. Zero disables prefetch. The default is 0 (no prefetch).
//
// Note: this option is not applied uniformly by every ExecuteQuery-based API.
// It affects only response consumers that use the response-part streaming path
// with prefetch support; other consumers may ignore it.
func WithResponsePartPrefetch(parts int) ExecuteOption {
	return options.WithResponsePartPrefetch(parts)
}

// WithConcurrentResultSets is deprecated and has no effect.
//
// Client.Query always enables concurrent result sets internally because it materializes the full response.
//
// Deprecated: WithConcurrentResultSets is deprecated and has no effect.
func WithConcurrentResultSets(bool) ExecuteOption {
	return options.NOP()
}

func WithCallOptions(opts ...grpc.CallOption) ExecuteOption {
	return options.WithCallOptions(opts...)
}

// WithResourcePool is an option for define resource pool for execute query
//
// Read more https://ydb.tech/docs/ru/dev/resource-consumption-management
func WithResourcePool(id string) ExecuteOption {
	return options.WithResourcePool(id)
}
