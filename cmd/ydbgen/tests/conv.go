package tests

//go:generate ydbgen -ignore "_test.go$"

//ydb:generate scan
type ConvAssert struct {
	Int8Int16   int16 `ydb:"type:int8,conv:assert"`
	Int32Int64  int64 `ydb:"type:int32,conv:assert"`
	Int16Int8   int8  `ydb:"type:int16,conv:assert"`
	Uint64Int8  int8  `ydb:"type:uint64,conv:assert"`
	Uint32Uint  uint  `ydb:"type:uint32,conv:assert"`
	Int32Int    int   `ydb:"type:int32,conv:assert"`
	Int32ToByte byte  `ydb:"type:int32,conv:assert"`
}
