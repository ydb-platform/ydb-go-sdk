package main

//go:generate ydbgen

// User represents a user of application.
//ydb:scan,params,container
type User struct {
	Name        string `ydb:"column:name,type:utf8"`
	Surname     string `ydb:"type:utf8"`
	Height      int8   `ydb:"type:uint64,conv:assert"`
	Weight      int8   `ydb:"type:uint8,conv:unsafe"`
	SomeUTFData []byte
	Age         int64 `ydb:"age"`
	Ignore      ID    `ydb:"-"`
}

//ydb:scan
type Car struct {
	Engine string
	Weight int `ydb:"type:int64,conv:unsafe"`
}

//ydb:scan
type Book struct {
	Title string `ydb:"pos:2"`
	Pages int    `ydb:"pos:1,type:int64,conv:unsafe"`
	Stars uint8  `ydb:"pos:3,type:uint16,conv:unsafe"`
}

//ydb:scan
type Users []User

func main() {
	//
}
