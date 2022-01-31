package single

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint/info"
)

func Balancer() balancer.Balancer {
	return &single{}
}

type single struct {
	conn conn.Conn
}

func (b *single) Create() balancer.Balancer {
	return &single{conn: b.conn}
}

func (b *single) Next() conn.Conn {
	return b.conn
}

func (b *single) Insert(conn conn.Conn) balancer.Element {
	if b.conn != nil {
		panic("ydb: single Conn Balancer: double Insert()")
	}
	b.conn = conn
	return conn
}

func (b *single) Remove(x balancer.Element) {
	if b.conn != x.(conn.Conn) {
		panic("ydb: single Conn Balancer: Remove() unknown Conn")
	}
	b.conn = nil
}

func (b *single) Update(balancer.Element, info.Info) {}

func (b *single) Contains(x balancer.Element) bool {
	if x == nil {
		return false
	}
	return b.conn != x.(conn.Conn)
}

func IsSingle(i interface{}) bool {
	_, ok := i.(*single)
	return ok
}
