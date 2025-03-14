package ydb

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/params"

// ParamsBuilder used for create query arguments instead of tons options.
func ParamsBuilder() params.Builder {
	return params.Builder{}
}
