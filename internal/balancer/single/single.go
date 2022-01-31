package single

import (
	balancer2 "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint/info"
)

func Balancer() balancer2.Balancer {
	return &balancer{}
}

type balancer struct {
	conn conn.Conn
}

func (b *balancer) Create() balancer2.Balancer {
	return &balancer{conn: b.conn}
}

func (b *balancer) Next() conn.Conn {
	return b.conn
}

func (b *balancer) Insert(conn conn.Conn) balancer2.Element {
	if b.conn != nil {
		panic("ydb: single Conn Balancer: double Insert()")
	}
	b.conn = conn
	return conn
}

func (b *balancer) Remove(x balancer2.Element) {
	if b.conn != x.(conn.Conn) {
		panic("ydb: single Conn Balancer: Remove() unknown Conn")
	}
	b.conn = nil
}

func (b *balancer) Update(balancer2.Element, info.Info) {}

func (b *balancer) Contains(x balancer2.Element) bool {
	if x == nil {
		return false
	}
	return b.conn != x.(conn.Conn)
}

func IsSingle(i interface{}) bool {
	_, ok := i.(*balancer)
	return ok
}
