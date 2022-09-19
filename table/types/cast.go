//go:build !go1.18
// +build !go1.18

package types

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/value"

func CastTo(v Value, dst interface{}) error {
	return value.CastTo(v, dst)
}
