//go:build go1.18
// +build go1.18

package types

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/value"

type CastDestinationType interface {
	*string | *[]byte | *uint64 | *uint32 | *uint16 | *uint8 | *int64 | *int32 | *int16 | *int8 | *float64 | *float32
}

func CastTo[T CastDestinationType](v Value, dst T) error {
	return value.CastTo(v, dst)
}
