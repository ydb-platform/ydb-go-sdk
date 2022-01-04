package single

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/iface"
)

func Balancer() iface.Balancer {
	return &singleConnBalancer{}
}

type singleConnBalancer struct {
	conn conn.Conn
}

func (s *singleConnBalancer) Next() conn.Conn {
	return s.conn
}

func (s *singleConnBalancer) Insert(conn conn.Conn) iface.Element {
	if s.conn != nil {
		panic("ydb: single Conn Balancer: double Insert()")
	}
	s.conn = conn
	return conn
}

func (s *singleConnBalancer) Remove(x iface.Element) {
	if s.conn != x.(conn.Conn) {
		panic("ydb: single Conn Balancer: Remove() unknown Conn")
	}
	s.conn = nil
}

func (s *singleConnBalancer) Update(iface.Element, info.Info) {}

func (s *singleConnBalancer) Contains(x iface.Element) bool {
	if x == nil {
		return false
	}
	return s.conn != x.(conn.Conn)
}
