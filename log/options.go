package log

import "github.com/ydb-platform/ydb-go-sdk/v3/log/structural"

func WithLogQuery() structural.Option {
	return structural.WithLogQuery()
}
