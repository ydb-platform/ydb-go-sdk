package ydb

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

type stubBalancer struct {
	OnNext   func() *conn
	OnInsert func(*conn, connInfo) balancerElement
	OnUpdate func(balancerElement, connInfo)
	OnRemove func(balancerElement)
}

func simpleBalancer() (*connList, balancer) {
	cs := new(connList)
	var i int
	return cs, stubBalancer{
		OnNext: func() *conn {
			n := len(*cs)
			if n == 0 {
				return nil
			}
			e := (*cs)[i%n]
			i++
			return e.conn
		},
		OnInsert: func(conn *conn, info connInfo) balancerElement {
			return cs.Insert(conn, info)
		},
		OnRemove: func(x balancerElement) {
			e := x.(*connListElement)
			cs.Remove(e)
		},
		OnUpdate: func(x balancerElement, info connInfo) {
			e := x.(*connListElement)
			e.info = info
		},
	}
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

type stubListener struct {
	C chan net.Conn // Client half of the connection.
}

func newStubListener() *stubListener {
	return &stubListener{
		C: make(chan net.Conn),
	}
}

func (ln *stubListener) Accept() (net.Conn, error) {
	s, c := net.Pipe()
	ln.C <- c
	return s, nil
}

func (ln *stubListener) Addr() net.Addr {
	return &net.TCPAddr{}
}

func (ln *stubListener) Close() error {
	return nil
}

func (ln *stubListener) Dial(ctx context.Context) (*grpc.ClientConn, error) {
	//nolint:SA1019
	return grpc.Dial("",
		grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
			select {
			case c := <-ln.C:
				return c, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}),
		grpc.WithInsecure(),
	)
}

func TestClusterAwait(t *testing.T) {
	const timeout = 100 * time.Millisecond

	ln := newStubListener()
	srv := grpc.NewServer()
	go srv.Serve(ln)

	var connToReturn *conn
	c := &cluster{
		dial: func(ctx context.Context, _ string, _ int) (_ *conn, err error) {
			cc, err := ln.Dial(ctx)
			if err != nil {
				return nil, err
			}
			return &conn{
				conn: cc,
			}, nil
		},
		balancer: stubBalancer{
			OnInsert: func(c *conn, _ connInfo) balancerElement {
				connToReturn = c
				return c.addr
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
