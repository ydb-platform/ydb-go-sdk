package tests

//ydb:generate params
type Params struct {
	Name          string
	Int16ToUint32 int16 `ydb:"types:uint32,conv:assert"`
	IntToInt64    int   `ydb:"types:int64,conv:assert"`
}
