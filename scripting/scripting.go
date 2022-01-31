// nolint:revive
package ydb_scripting

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type ExplainMode = uint8

const (
	ExplainModeUnknown ExplainMode = iota
	ExplainModeValidate
	ExplainModePlan

	ExplainModeDefault = ExplainModePlan
)

type Client interface {
	closer.Closer

	Execute(
		ctx context.Context,
		query string,
		params *ydb_table.QueryParameters,
	) (ydb_table_result.Result, error)
	Explain(
		ctx context.Context,
		query string,
		mode ExplainMode,
	) (ydb_table.ScriptingYQLExplanation, error)
	StreamExecute(
		ctx context.Context,
		query string,
		params *ydb_table.QueryParameters,
	) (ydb_table_result.StreamResult, error)
}
