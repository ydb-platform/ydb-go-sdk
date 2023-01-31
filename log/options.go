package log

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/logs/traces"

func WithLogQuery() traces.Option {
	return traces.WithLogQuery()
}
