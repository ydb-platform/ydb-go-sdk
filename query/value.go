package query

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type Value = value.Value

func BoolValue(v bool) Value {
	return value.BoolValue(v)
}

func Uint64Value(v uint64) Value {
	return value.Uint64Value(v)
}

func Int64Value(v int64) Value {
	return value.Int64Value(v)
}

func Uint32Value(v uint32) Value {
	return value.Uint32Value(v)
}

func Int32Value(v int32) Value {
	return value.Int32Value(v)
}

func Uint16Value(v uint16) Value {
	return value.Uint16Value(v)
}

func Int16Value(v int16) Value {
	return value.Int16Value(v)
}

func Uint8Value(v uint8) Value {
	return value.Uint8Value(v)
}

func Int8Value(v int8) Value {
	return value.Int8Value(v)
}

func TextValue(v string) Value {
	return value.TextValue(v)
}

func BytesValue(v []byte) Value {
	return value.BytesValue(v)
}

func IntervalValue(v time.Duration) Value {
	return value.IntervalValueFromDuration(v)
}

func TimestampValue(v time.Time) Value {
	return value.TimestampValueFromTime(v)
}

func DatetimeValue(v time.Time) Value {
	return value.DatetimeValueFromTime(v)
}
