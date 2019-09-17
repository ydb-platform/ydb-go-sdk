package tests

//ydb:generate
type Container struct {
	String      string
	IntToUint64 int `ydb:"type:uint64,conv:assert"`
	Structs     []Foo
	Bytes       []byte `ydb:"type:string"`
}

//ydb:generate
type Foo struct {
	ID   string
	Ints []int32
}

//ydb:generate
type Bar [][][]string
