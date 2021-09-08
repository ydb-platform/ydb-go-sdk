package tests

import "github.com/ydb-platform/ydb-go-sdk/v3/opt"

//ydb:generate value,scan,params,type
type Optional struct {
	Int64 opt.Int64  `ydb:"type:int16?,conv:assert"`
	Str   opt.String `ydb:"type:string"`
	Int32 int32
}
