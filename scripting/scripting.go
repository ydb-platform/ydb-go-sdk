package scripting

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
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
	Execute(
		ctx context.Context,
		query string,
		params *params.Parameters,
	) (result.Result, error)
	Explain(
		ctx context.Context,
		query string,
		mode ExplainMode,
	) (table.ScriptingYQLExplanation, error)
	StreamExecute(
		ctx context.Context,
		query string,
		params *params.Parameters,
	) (result.StreamResult, error)
}
