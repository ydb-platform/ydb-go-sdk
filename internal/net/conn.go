package net

import (
	"context"
	"net"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type ctxAddressKey struct{}

func WithAddress(ctx context.Context, address string) context.Context {
	return context.WithValue(ctx, ctxAddressKey{}, address)
}

func contextAddress(ctx context.Context) string {
	if v, ok := ctx.Value(ctxAddressKey{}).(string); ok {
		return v
	}
	return ""
}

func New(
	ctx context.Context,
	resolvedAddress string,
	t trace.Driver,
) (_ net.Conn, err error) {
	address := contextAddress(ctx)
	onDone := trace.DriverOnNetDial(t, &ctx, address, resolvedAddress)
	defer func() {
		onDone(err)
	}()
	cc, err := (&net.Dialer{}).DialContext(ctx, "tcp", resolvedAddress)
	if err != nil {
		return nil, err
	}
	return &netConn{
		address:         address,
		resolvedAddress: resolvedAddress,
		cc:              cc,
		trace:           t,
	}, nil
}

type netConn struct {
	address         string
	resolvedAddress string
	trace           trace.Driver
	cc              net.Conn
}

func (c *netConn) Read(b []byte) (n int, err error) {
	onDone := trace.DriverOnNetRead(c.trace, c.address, c.resolvedAddress, len(b))
	defer func() {
		onDone(n, err)
	}()
	return c.cc.Read(b)
}

func (c *netConn) Write(b []byte) (n int, err error) {
	onDone := trace.DriverOnNetWrite(c.trace, c.address, c.resolvedAddress, len(b))
	defer func() {
		onDone(n, err)
	}()
	return c.cc.Write(b)
}

func (c *netConn) Close() (err error) {
	onDone := trace.DriverOnNetClose(c.trace, c.address, c.resolvedAddress)
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
