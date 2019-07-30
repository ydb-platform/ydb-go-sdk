package tests

import "github.com/yandex-cloud/ydb-go-sdk/opt"

//go:generate ydbgen -ignore "_test.go$"

//ydb:generate value,scan,params,type
type Optional struct {
	Int64 opt.Int64  `ydb:"type:int16?,conv:assert"`
	Str   opt.String `ydb:"type:string"`
	Int32 int32
}
