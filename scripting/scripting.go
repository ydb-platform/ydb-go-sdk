package scripting

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
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

	ExecuteYql(
		ctx context.Context,
		query string,
		params *table.QueryParameters,
	) (result.Result, error)
	ExplainYql(
		ctx context.Context,
		query string,
		mode ExplainMode,
	) (table.ScriptingYQLExplanation, error)
	StreamExecuteYql(
		ctx context.Context,
		query string,
		params *table.QueryParameters,
	) (result.StreamResult, error)
}
