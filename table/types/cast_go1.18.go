//go:build go1.18
// +build go1.18

package types

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/value"

type supportedCastTypes interface {
	~string | ~[]byte | ~uint64 | ~uint32 | ~uint16 | ~uint8 | ~int64 | ~int32 | ~int16 | ~int8 | ~float64 | ~float32
}

// CastTo casts Value to destination type as possible
func CastTo[T supportedCastTypes](v Value, dst *T) error {
	return value.CastTo(v, dst)
}
