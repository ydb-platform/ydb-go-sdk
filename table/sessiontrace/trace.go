package sessiontrace

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"

type Trace struct {
	table.Trace
}

type SessionPoolTrace table.SessionPoolTrace
