package tests

//ydb:generate value,scan,params,types
type Optional struct {
	Int64 int64  `ydb:"types:int16?,conv:assert"`
	Str   string `ydb:"types:string"`
	Int32 int32
}
