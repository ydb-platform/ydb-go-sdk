package conn

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
)

type QueryMode = xcontext.QueryMode

const (
	UnknownQueryMode   = xcontext.UnknownQueryMode
	DataQueryMode      = xcontext.DataQueryMode
	ExplainQueryMode   = xcontext.ExplainQueryMode
	ScanQueryMode      = xcontext.ScanQueryMode
	SchemeQueryMode    = xcontext.SchemeQueryMode
	ScriptingQueryMode = xcontext.UnknownQueryMode

	DefaultQueryMode = xcontext.UnknownQueryMode
)
