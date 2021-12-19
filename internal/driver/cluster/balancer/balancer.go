package balancer

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
)

var (
	// ErrNilBalancerElement returned when requested on a nil Balancer element.
	ErrNilBalancerElement = errors.New("nil balancer element")
	// ErrUnknownBalancerElement returned when requested on a unknown Balancer element.
	ErrUnknownBalancerElement = errors.New("unknown balancer element")
	// ErrUnknownTypeOfBalancerElement returned when requested on a unknown types of Balancer element.
	ErrUnknownTypeOfBalancerElement = errors.New("unknown types of balancer element")
)

// Element is an empty interface that holds some Balancer specific
// data.
type Element interface {
}

// Balancer is an interface that implements particular load-balancing
// algorithm.
//
// Balancer methods called synchronized. That is, implementations must not
// provide additional goroutine safety.
type Balancer interface {
	// Next returns next connection for request.
	// Next MUST not return nil if it has at least one connection.
	Next() conn.Conn

	// Insert inserts new connection.
	Insert(conn.Conn, info.Info) Element

	// Update updates previously inserted connection.
	Update(Element, info.Info)

	// Remove removes previously inserted connection.
	Remove(Element)

	// Contains returns true if Balancer contains requested element.
	Contains(Element) bool
}

func defaultBalancer() Balancer {
	return &randomChoice{}
}

func newBalancer(cfg config.BalancerConfig) Balancer {
	switch cfg.Algorithm {
	case config.BalancingAlgorithmRoundRobin:
		return &roundRobin{}
	case config.BalancingAlgorithmRandomChoice:
		return &randomChoice{}
	default:
		return defaultBalancer()
	}
}

func New(cfg config.BalancerConfig) Balancer {
	if !cfg.PreferLocal {
		return newBalancer(cfg)
	}
	return NewMultiBalancer(
		WithBalancer(
			newBalancer(cfg), func(_ conn.Conn, info info.Info) bool {
				return info.Local
			},
		),
		WithBalancer(
			newBalancer(cfg), func(_ conn.Conn, info info.Info) bool {
				return !info.Local
			},
		),
	)
}

func Single() Balancer {
	return &singleConnBalancer{}
}

type multiHandle struct {
	elements []Element
}

type multiBalancer struct {
	balancer []Balancer
	filter   []func(conn.Conn, info.Info) bool
}

func WithBalancer(b Balancer, filter func(conn.Conn, info.Info) bool) balancerOption {
	return func(m *multiBalancer) {
		m.balancer = append(m.balancer, b)
		m.filter = append(m.filter, filter)
	}
}

type balancerOption func(*multiBalancer)

func NewMultiBalancer(opts ...balancerOption) *multiBalancer {
	m := new(multiBalancer)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *multiBalancer) Contains(x Element) bool {
	for i, x := range x.(multiHandle).elements {
		if x == nil {
			continue
		}
		if m.balancer[i].Contains(x) {
			return true
		}
	}
	return false
}

func (m *multiBalancer) Next() conn.Conn {
	for _, b := range m.balancer {
		if c := b.Next(); c != nil {
			return c
		}
	}
	return nil
}

func (m *multiBalancer) Insert(conn conn.Conn, info info.Info) Element {
	n := len(m.filter)
	h := multiHandle{
		elements: make([]Element, n),
	}
	for i, f := range m.filter {
		if f(conn, info) {
			x := m.balancer[i].Insert(conn, info)
			h.elements[i] = x
		}
	}
	return h
}

func (m *multiBalancer) Update(x Element, info info.Info) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			m.balancer[i].Update(x, info)
		}
	}
}

func (m *multiBalancer) Remove(x Element) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			m.balancer[i].Remove(x)
		}
	}
}

type singleConnBalancer struct {
	conn conn.Conn
}

func (s *singleConnBalancer) Next() conn.Conn {
	return s.conn
}
func (s *singleConnBalancer) Insert(conn conn.Conn, _ info.Info) Element {
	if s.conn != nil {
		panic("ydb: single Conn Balancer: double Insert()")
	}
	s.conn = conn
	return conn
}
func (s *singleConnBalancer) Remove(x Element) {
	if s.conn != x.(conn.Conn) {
		panic("ydb: single Conn Balancer: Remove() unknown Conn")
	}
	s.conn = nil
}
func (s *singleConnBalancer) Update(Element, info.Info) {}
func (s *singleConnBalancer) Contains(x Element) bool {
	if x == nil {
		return false
	}
	return s.conn != x.(conn.Conn)
}
