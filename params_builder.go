package ydb

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/params"

// ParamsBuilder used for create query arguments instead of tons options.
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func ParamsBuilder() params.Builder {
	return params.Builder{}
}
