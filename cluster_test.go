package ydb

import (
	"context"
	"testing"
	"time"
)

func TestClusterAwait(t *testing.T) {
	const timeout = 100 * time.Millisecond
	c := &cluster{
		dial: func(context.Context, string, int) (*conn, error) {
			return new(conn), nil
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

		c.Upsert(context.Background(), Endpoint{})
		assertRecvError(t, timeout, got, nil)
	}
}

func TestClusterBalance(t *testing.T) {
	for _, test := range []struct {
		name   string
		add    []Endpoint
		del    []string
		repeat int
		exp    map[string]int
		err    bool
	}{
		{
			add: []Endpoint{
				{Addr: "foo"},
				{Addr: "bar"},
			},
			repeat: 1000,
			exp: map[string]int{
				"foo": 500,
				"bar": 500,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 0.25},
				{Addr: "bar", LoadFactor: 1},
				{Addr: "baz", LoadFactor: 1},
			},
			repeat: 1000,
			exp: map[string]int{
				"foo": 500,
				"bar": 250,
				"baz": 250,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 1},
				{Addr: "bar", LoadFactor: 0.25},
				{Addr: "baz", LoadFactor: 0.25},
			},
			repeat: 1000,
			exp: map[string]int{
				"foo": 200,
				"bar": 400,
				"baz": 400,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 0.25},
				{Addr: "bar", LoadFactor: 1},
				{Addr: "baz", LoadFactor: 1},
			},
			del:    []string{"foo"},
			repeat: 1000,
			exp: map[string]int{
				"bar": 500,
				"baz": 500,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 1},
				{Addr: "bar", LoadFactor: 0.25},
				{Addr: "baz", LoadFactor: 0.25},
			},
			del:    []string{"foo"},
			repeat: 1000,
			exp: map[string]int{
				"bar": 500,
				"baz": 500,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 1},
				{Addr: "bar", LoadFactor: 0.75},
				{Addr: "baz", LoadFactor: 0.25},
			},
			del:    []string{"bar"},
			repeat: 1000,
			exp: map[string]int{
				"foo": 250,
				"baz": 750,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 0},
				{Addr: "bar", LoadFactor: 0},
				{Addr: "baz", LoadFactor: 0},
			},
			del:    []string{"baz"},
			repeat: 1000,
			exp: map[string]int{
				"foo": 500,
				"bar": 500,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 0},
				{Addr: "bar", LoadFactor: 0},
				{Addr: "baz", LoadFactor: 0},
			},
			del:    []string{"baz", "foo", "bar"},
			repeat: 1,
			err:    true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				mconn = map[*conn]string{} // conn to addr mapping for easy matching.
				maddr = map[string]*conn{} // addr to conn mapping.
			)
			c := &cluster{
				dial: func(_ context.Context, host string, port int) (*conn, error) {
					addr := connAddr{
						addr: host,
						port: port,
					}
					c := &conn{
						addr: addr,
					}
					mconn[c] = host
					maddr[host] = c
					return c, nil
				},
			}
			for _, e := range test.add {
				if err := c.Upsert(context.Background(), e); err != nil {
					t.Fatal(err)
				}
			}
			for _, x := range test.del {
				if !c.Remove(maddr[x].addr) {
					t.Fatalf("can not delete endpoint %q", x)
				}
			}

			// Prepare canceled context to not stuck on awaiting non-existing
			// connection.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			dist := map[string]int{}
			for i := 0; i < test.repeat; i++ {
				conn, err := c.Get(ctx)
				if test.err {
					if err != nil {
						break
					}
					t.Fatalf("unexpected nil error")
				} else if err != nil {
					t.Fatal(err)
				}
				addr, has := mconn[conn]
				if !has {
					t.Fatalf("received unknown conn")
				}
				dist[addr]++
			}

			for host, exp := range test.exp {
				if act := dist[host]; act != exp {
					t.Errorf(
						"unexpected distribution for host %q: %v; want %v",
						host, act, exp,
					)
				}
				delete(dist, host)
			}
			for host := range dist {
				t.Fatalf("unexpected host in distribution: %q", host)
			}
		})
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
