package tests

//ydb:generate scan
type ConvAssert struct {
	Int32ToByte byte  `ydb:"types:int32,conv:assert"`
	Int16Int8   int8  `ydb:"types:int16,conv:assert"`
	Uint64Int8  int8  `ydb:"types:uint64,conv:assert"`
	Int8Int16   int16 `ydb:"types:int8,conv:assert"`
	Uint32Uint  uint  `ydb:"types:uint32,conv:assert"`
	Int32Int    int   `ydb:"types:int32,conv:assert"`
	Int32Int64  int64 `ydb:"types:int32,conv:assert"`
}
