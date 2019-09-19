package tests

//ydb:generate
type Container struct {
	Structs []Foo
	Bytes   []byte   `ydb:"type:list<uint32>,conv:assert"`
	Strings []string `ydb:"type:list<string>"`
	String  []byte
}

//ydb:generate
type Foo struct {
	ID   string
	Ints []int32
}

//ydb:generate scan
type Foos []Foo

//ydb:generate scan
type Bar [][][]string
