package list

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

type Element struct {
	Index int
	Conn  conn.Conn
}

type List []*Element

func (cs *List) Insert(cc conn.Conn) *Element {
	e := &Element{
		Index: len(*cs),
		Conn:  cc,
	}
	*cs = append(*cs, e)
	return e
}

func (cs *List) Remove(x *Element) {
	l := *cs
	var (
		n    = len(l)
		last = l[n-1]
	)
	last.Index = x.Index
	l[x.Index], l[n-1] = l[n-1], nil
	l = l[:n-1]
	*cs = l
}

func (cs *List) Contains(x *Element) bool {
	l := *cs
	n := len(l)
	if x.Index >= n {
		return false
	}
	return l[x.Index] == x
}
