package ydb

// balancerElement is an empty interface that holds some balancer specific
// data.
type balancerElement interface {
}

// balancer is an interface that implements particular load-balancing
// algorithm.
//
// balancer methods called synchronized. That is, implementations must not
// provide additional goroutine safety.
type balancer interface {
	// Next returns next connection for request.
	// Next MUST not return nil if it has at least one connection.
	Next() *conn

	// Insert inserts new connection.
	Insert(*conn, connInfo) balancerElement

	// Update updates previously inserted connection.
	Update(balancerElement, connInfo)

	// Remove removes previously inserted connection.
	Remove(balancerElement)
}

type multiHandle struct {
	elements []balancerElement
}

type multiBalancer struct {
	balancer []balancer
	filter   []func(*conn, connInfo) bool
}

func withBalancer(b balancer, filter func(*conn, connInfo) bool) balancerOption {
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

func (m *multiBalancer) Next() *conn {
	for _, b := range m.balancer {
		if c := b.Next(); c != nil {
			return c
		}
	}
	return nil
}

func (m *multiBalancer) Insert(conn *conn, info connInfo) balancerElement {
	n := len(m.filter)
	h := multiHandle{
		elements: make([]balancerElement, n),
	}
	for i, f := range m.filter {
		if f(conn, info) {
			x := m.balancer[i].Insert(conn, info)
			h.elements[i] = x
		}
	}
	return h
}

func (m *multiBalancer) Update(x balancerElement, info connInfo) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			m.balancer[i].Update(x, info)
		}
	}
}

func (m *multiBalancer) Remove(x balancerElement) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			m.balancer[i].Remove(x)
		}
	}
}
