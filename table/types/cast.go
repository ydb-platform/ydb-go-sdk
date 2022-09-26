package types

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/value"

// CastTo try cast value to destination type value
func CastTo(v Value, dst interface{}) error {
	return value.CastTo(v, dst)
}
