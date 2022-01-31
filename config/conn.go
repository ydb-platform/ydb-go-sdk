// nolint:revive
package ydb_config

import (
	"context"
	"net"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type netConn struct {
	address string
	trace   ydb_trace.Driver
	cc      net.Conn
}

func newConn(ctx context.Context, address string, t ydb_trace.Driver) (_ net.Conn, err error) {
	onDone := ydb_trace.DriverOnNetDial(t, &ctx, address)
	defer func() {
		onDone(err)
	}()
	cc, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	return &netConn{
		address: address,
		cc:      cc,
		trace:   t,
	}, nil
}

func (c *netConn) Read(b []byte) (n int, err error) {
	onDone := ydb_trace.DriverOnNetRead(c.trace, c.address, len(b))
	defer func() {
		onDone(n, err)
	}()
	return c.cc.Read(b)
}

func (c *netConn) Write(b []byte) (n int, err error) {
	onDone := ydb_trace.DriverOnNetWrite(c.trace, c.address, len(b))
	defer func() {
		onDone(n, err)
	}()
	return c.cc.Write(b)
}

func (c *netConn) Close() (err error) {
	onDone := ydb_trace.DriverOnNetClose(c.trace, c.address)
	defer func() {
		onDone(err)
	}()
	return c.cc.Close()
}

func (c *netConn) LocalAddr() net.Addr {
	return c.cc.LocalAddr()
}

func (c *netConn) RemoteAddr() net.Addr {
	return c.cc.RemoteAddr()
}

func (c *netConn) SetDeadline(t time.Time) error {
	return c.cc.SetDeadline(t)
}

func (c *netConn) SetReadDeadline(t time.Time) error {
	return c.cc.SetReadDeadline(t)
}

func (c *netConn) SetWriteDeadline(t time.Time) error {
	return c.cc.SetWriteDeadline(t)
}
