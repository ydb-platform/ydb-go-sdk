package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
)

type (
	Field = kv.KeyValue
)

const (
	IntType      = kv.IntType
	Int64Type    = kv.Int64Type
	StringType   = kv.StringType
	BoolType     = kv.BoolType
	DurationType = kv.DurationType
	StringsType  = kv.StringsType
	ErrorType    = kv.ErrorType
	AnyType      = kv.AnyType
	StringerType = kv.StringerType
)

func appendFieldByCondition(condition bool, ifTrueField Field, fields ...Field) []Field {
	if condition {
		fields = append(fields, ifTrueField)
	}

	return fields
}
