package tests

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/opt"
)

//ydb:generate value,scan,params,types
type Optional struct {
	Int64 opt.Int64  `ydb:"types:int16?,conv:assert"`
	Str   opt.String `ydb:"types:string"`
	Int32 int32
}
