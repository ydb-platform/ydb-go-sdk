package spans

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
	StringsType  = kv.StringsType
	StringerType = kv.StringerType
)
