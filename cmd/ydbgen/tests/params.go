package tests

//go:generate ydbgen -ignore "_test.go$"

//ydb:generate params
type Params struct {
	Name          string
	Int16ToUint32 int16 `ydb:"type:uint32,conv:assert"`
	IntToInt64    int   `ydb:"type:int64,conv:assert"`
}
