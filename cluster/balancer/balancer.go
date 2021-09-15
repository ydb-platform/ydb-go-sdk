package balancer

import (
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn/info"
	"math/rand"
	"strings"
	"time"
)

var (
	// ErrNilBalancerElement returned when requested on a nil Balancer element.
	ErrNilBalancerElement = errors.New("nil Balancer element")
	// ErrUnknownBalancerElement returned when requested on a unknown Balancer element.
	ErrUnknownBalancerElement = errors.New("unknown Balancer element")
	// ErrUnknownTypeOfBalancerElement returned when requested on a unknown types of Balancer element.
	ErrUnknownTypeOfBalancerElement = errors.New("unknown types of Balancer element")
)

// BalancerElement is an empty interface that holds some Balancer specific
// data.
type BalancerElement interface {
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
	Insert(conn.Conn, info.Info) BalancerElement

	// Update updates previously inserted connection.
	Update(BalancerElement, info.Info)

	// Remove removes previously inserted connection.
	Remove(BalancerElement)

	// Pessimize pessimizes some Balancer element.
	Pessimize(BalancerElement) error

	// Contains returns true if Balancer contains requested element.
	Contains(BalancerElement) bool
}

func defaultBalancer() Balancer {
	return &randomChoice{
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func newBalancer(cfg Config) Balancer {
	switch cfg.Algorithm {
	case RoundRobin:
		return &roundRobin{}
	case P2C:
		return &p2c{
			Criterion: connRuntimeCriterion{
				PreferLocal:     cfg.PreferLocal,
				OpTimeThreshold: cfg.OpTimeThreshold,
			},
		}
	case RandomChoice:
		return &randomChoice{
			r: rand.New(rand.NewSource(time.Now().UnixNano())),
		}
	default:
		return defaultBalancer()
	}
}

func New(cfg Config) Balancer {
	if !cfg.PreferLocal {
		return newBalancer(cfg)
	}
	return newMultiBalancer(
		withBalancer(
			newBalancer(cfg), func(_ conn.Conn, info info.Info) bool {
				return info.Local
			},
		),
		withBalancer(
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
	elements []BalancerElement
}

type multiBalancer struct {
	balancer []Balancer
	filter   []func(conn.Conn, info.Info) bool
}

func withBalancer(b Balancer, filter func(conn.Conn, info.Info) bool) balancerOption {
	return func(m *multiBalancer) {
		m.balancer = append(m.balancer, b)
		m.filter = append(m.filter, filter)
	}
}

type balancerOption func(*multiBalancer)

func newMultiBalancer(opts ...balancerOption) *multiBalancer {
	m := new(multiBalancer)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *multiBalancer) Contains(x BalancerElement) bool {
	for _, b := range m.balancer {
		if b.Contains(x) {
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

func (m *multiBalancer) Insert(conn conn.Conn, info info.Info) BalancerElement {
	n := len(m.filter)
	h := multiHandle{
		elements: make([]BalancerElement, n),
	}
	for i, f := range m.filter {
		if f(conn, info) {
			x := m.balancer[i].Insert(conn, info)
			h.elements[i] = x
		}
	}
	return h
}

func (m *multiBalancer) Update(x BalancerElement, info info.Info) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			m.balancer[i].Update(x, info)
		}
	}
}

func (m *multiBalancer) Remove(x BalancerElement) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			m.balancer[i].Remove(x)
		}
	}
}

func (m *multiBalancer) Pessimize(x BalancerElement) error {
	if x == nil {
		return ErrNilBalancerElement
	}
	good := 0
	all := 0
	errs := make([]string, 0)
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			all++
			if e := m.balancer[i].Pessimize(x); e == nil {
				good++
			} else if !errors.Is(e, ErrUnknownBalancerElement) { // collect error only if not an ErrUnknownBalancerElement
				errs = append(errs, e.Error())
			}
		}
	}
	if good > 0 || all == 0 {
		return nil
	}
	return fmt.Errorf("[multiBalancer] unknown Balancer element %+v; errors: [ %s ]", x, strings.Join(errs, "; "))
}

type singleConnBalancer struct {
	conn conn.Conn
}

func (s *singleConnBalancer) Next() conn.Conn {
	return s.conn
}
func (s *singleConnBalancer) Insert(conn conn.Conn, _ info.Info) BalancerElement {
	if s.conn != nil {
		panic("ydb: single Conn Balancer: double Insert()")
	}
	s.conn = conn
	return conn
}
func (s *singleConnBalancer) Remove(x BalancerElement) {
	if s.conn != x.(conn.Conn) {
		panic("ydb: single Conn Balancer: Remove() unknown Conn")
	}
	s.conn = nil
}
func (s *singleConnBalancer) Update(BalancerElement, info.Info)  {}
func (s *singleConnBalancer) Pessimize(el BalancerElement) error { return nil }
func (s *singleConnBalancer) Contains(x BalancerElement) bool {
	if x == nil {
		return false
	}
	return s.conn != x.(conn.Conn)
}
