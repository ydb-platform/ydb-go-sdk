package ydb

import (
	"context"
	"testing"
	"time"
)

type stubBalancer struct {
	OnNext   func() *conn
	OnInsert func(*conn, connInfo) balancerElement
	OnUpdate func(balancerElement, connInfo)
	OnRemove func(balancerElement)
}

func (s stubBalancer) Next() *conn {
	if f := s.OnNext; f != nil {
		return f()
	}
	return nil
}
func (s stubBalancer) Insert(c *conn, i connInfo) balancerElement {
	if f := s.OnInsert; f != nil {
		return f(c, i)
	}
	return nil
}
func (s stubBalancer) Update(el balancerElement, i connInfo) {
	if f := s.OnUpdate; f != nil {
		f(el, i)
	}
}
func (s stubBalancer) Remove(el balancerElement) {
	if f := s.OnRemove; f != nil {
		f(el)
	}
}

func TestClusterAwait(t *testing.T) {
	const timeout = 100 * time.Millisecond

	var connToReturn *conn
	c := &cluster{
		dial: func(context.Context, string, int) (*conn, error) {
			return new(conn), nil
		},
		balancer: stubBalancer{
			OnInsert: func(c *conn, _ connInfo) balancerElement {
				connToReturn = c
				return nil
			},
			OnNext: func() *conn {
				return connToReturn
			},
		},
	}
	get := func() (<-chan error, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		got := make(chan error)
		go func() {
			_, err := c.Get(ctx)
			got <- err
		}()
		return got, cancel
	}
	{
		got, cancel := get()
		cancel()
		assertRecvError(t, timeout, got, context.Canceled)
	}
	{
		got, cancel := get()
		defer cancel()

		assertNoRecv(t, timeout, got)

		c.Insert(context.Background(), Endpoint{})
		assertRecvError(t, timeout, got, nil)
	}
}

func assertRecvError(t *testing.T, d time.Duration, e <-chan error, exp error) {
	select {
	case act := <-e:
		if act != exp {
			t.Errorf("%s: unexpected error: %v; want %v", fileLine(2), act, exp)
		}
	case <-time.After(d):
		t.Errorf("%s: nothing received after %s", fileLine(2), d)
	}
}
