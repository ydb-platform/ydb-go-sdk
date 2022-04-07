package xnet

import (
	"context"
	"fmt"
	"net"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type conn struct {
	net.Conn
	address string
	trace   trace.Driver
}

func New(ctx context.Context, address string, t trace.Driver) (_ net.Conn, err error) {
	onDone := trace.DriverOnNetDial(t, &ctx, address)
	defer func() {
		onDone(err)
	}()
	cc, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("%w: %s", err, address))
	}
	return &conn{
		Conn:    cc,
		address: address,
		trace:   t,
	}, nil
}

func (c *conn) Read(b []byte) (n int, err error) {
	onDone := trace.DriverOnNetRead(c.trace, c.address, len(b))
	defer func() {
		onDone(n, err)
	}()
	n, err = c.Conn.Read(b)
	if err != nil {
		return n, xerrors.WithStackTrace(err)
	}
	return n, nil
}

func (c *conn) Write(b []byte) (n int, err error) {
	onDone := trace.DriverOnNetWrite(c.trace, c.address, len(b))
	defer func() {
		onDone(n, err)
	}()
	n, err = c.Conn.Write(b)
	if err != nil {
		return n, xerrors.WithStackTrace(err)
	}
	return n, nil
}

func (c *conn) Close() (err error) {
	onDone := trace.DriverOnNetClose(c.trace, c.address)
	defer func() {
		onDone(err)
	}()
	err = c.Conn.Close()
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}
