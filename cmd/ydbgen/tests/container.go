package tests

//ydb:generate
type Container struct {
	Struct  Foo
	Structs []Foo
	Bytes   []byte   `ydb:"types:list<uint32>,conv:assert"`
	Strings []string `ydb:"types:list<string>"`
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
