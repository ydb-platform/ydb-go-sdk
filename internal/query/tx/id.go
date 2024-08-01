package tx

import "github.com/ydb-platform/ydb-go-sdk/v3/internal"

var _ Identifier = (*ID)(nil)

type ID struct {
	internal.InterfaceImplementation
	val string
}

func NewID(id string) ID {
	return ID{val: id}
}

func (id ID) ID() string {
	return id.val
}

func (id ID) isYdbTx() {}
