package ydbtest_test

import (
	"context"
	"fmt"
	"github.com/YandexDatabase/ydb-go-sdk/v3/traceutil"
	"log"
	"net"
	"testing"
	"time"

	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Table"
	"github.com/YandexDatabase/ydb-go-sdk/v3"
	"github.com/YandexDatabase/ydb-go-sdk/v3/internal/ydbtest"
	"github.com/YandexDatabase/ydb-go-sdk/v3/table"
)

func TestClusterTracking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := ydbtest.YDB{
		Database: "xxx",
		Handlers: ydbtest.Handlers{
			"/Ydb.Table.V1.TableService/CreateSession": ydbtest.SuccessHandler(
				ydbtest.Ident(&Ydb_Table.CreateSessionResult{}),
			),
		},
		T: t,
	}
	balancer := db.StartBalancer()
	defer balancer.Close()

	endpoint := db.StartEndpoint()
	defer endpoint.Close()

	var dtrace ydb.DriverTrace
	traceutil.Stub(&dtrace, func(name string, args ...interface{}) {
		log.Printf(
			"[driver] %s: %+v",
			name, traceutil.ClearContext(args),
		)
	})
	var (
		dialTicket = make(chan func(net.Conn) net.Conn, 1)
	)

	dialer := &ydb.Dialer{
		DriverConfig: &ydb.DriverConfig{
			Database:          "xxx",
			DiscoveryInterval: time.Hour,
			Trace:             dtrace,
		},
		NetDial: func(ctx context.Context, addr string) (net.Conn, error) {
			var wrap func(net.Conn) net.Conn
			if addr == balancer.Addr().String() {
				// Dialing for balancer.
				return balancer.DialContext(ctx)
			}

			select {
			// Dialing for endpoint.
			case wrap = <-dialTicket:
				conn, err := db.DialContext(ctx, addr)
				if err != nil {
					return nil, err
				}
				if wrap != nil {
					conn = wrap(conn)
				}
				return conn, nil

			default:
				return nil, fmt.Errorf("stub: kinda refused")
			}
		},
		Timeout: 250 * time.Millisecond,
	}
	d, err := dialer.Dial(ctx, balancer.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// At this point dialer reached balancer, received endpoints list and is
	// trying to connect to them (and is not able until we put ticket into the
	// dialTicket channel).

	tc := table.NewClient(d)
	mustCreateSession := func() {
		sub, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_, err = tc.CreateSession(sub)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	mustNotCreateSession := func() {
		sub, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		if _, err = tc.CreateSession(sub); err == nil {
			t.Fatalf("unexpected no error")
		}
	}

	// Try to execute some operation expecting that there are no any endpoint
	// connection.
	mustNotCreateSession()

	// Allow one connection to the endpoint.
	dialTicket <- nil
	conn := <-endpoint.ServerConn()

	// Execute operation to ensure that connection established.
	mustCreateSession()

	// Now close the connection to force driver to redial.
	_ = conn.Close()
	// Must not create session because there are no alive conns.
	mustNotCreateSession()

	// Allow one connection to the endpoint.
	dialTicket <- nil
	conn = <-endpoint.ServerConn()

	// Execute operation to ensure that connection established.
	mustCreateSession()

	// Now close the connection to force driver to redial.
	_ = conn.Close()
	// Must not create session because there are no alive conns.
	mustNotCreateSession()

	// Allow one connection to the endpoint. But for now return connection
	// which is actually "dead". That is, it will silently allow writes and
	// block on reads. This is simulation of, for example, endpoint process
	// killed by 9 without sending FIN packets to its connections.
	c := newConnProxy()
	dialTicket <- func(conn net.Conn) net.Conn {
		c.Conn = conn
		return c
	}
	<-endpoint.ServerConn()

	// Must not create session because there are no alive conns.
	mustNotCreateSession()

	// Allow the first read of connection handshake.
	c.ticket <- struct{}{}
	mustNotCreateSession()
}

type connProxy struct {
	net.Conn
	ticket chan struct{}
}

func newConnProxy() *connProxy {
	return &connProxy{
		ticket: make(chan struct{}),
	}
}

func (c *connProxy) Read(p []byte) (n int, err error) {
	<-c.ticket
	return c.Conn.Read(p)
}
