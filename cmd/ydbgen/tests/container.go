package tests

//ydb:generate value
type Container struct {
	String      string
	IntToUint64 int `ydb:"type:uint64,conv:assert"`
}
