package net

import (
	"context"
	"net"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type conn struct {
	address string
	trace   trace.Driver
	cc      net.Conn
}

func New(ctx context.Context, address string, t trace.Driver) (_ net.Conn, err error) {
	onDone := trace.DriverOnNetDial(t, &ctx, address)
	defer func() {
		onDone(err)
	}()
	cc, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, errors.Errorf(0, "dial failed: %w", err)
	}
	return &conn{
		address: address,
		cc:      cc,
		trace:   t,
	}, nil
}

func (c *conn) Read(b []byte) (n int, err error) {
	onDone := trace.DriverOnNetRead(c.trace, c.address, len(b))
	defer func() {
		onDone(n, err)
	}()
	n, err = c.cc.Read(b)
	if err != nil {
		return n, errors.Errorf(0, "read failed: %w", err)
	}
	return n, nil
}

func (c *conn) Write(b []byte) (n int, err error) {
	onDone := trace.DriverOnNetWrite(c.trace, c.address, len(b))
	defer func() {
		onDone(n, err)
	}()
	n, err = c.cc.Write(b)
	if err != nil {
		return n, errors.Errorf(0, "write failed: %w", err)
	}
	return n, nil
}

func (c *conn) Close() (err error) {
	onDone := trace.DriverOnNetClose(c.trace, c.address)
	defer func() {
		onDone(err)
	}()
	err = c.cc.Close()
	if err != nil {
		return errors.Errorf(0, "close failed: %w", err)
	}
	return nil
}

func (c *conn) LocalAddr() net.Addr {
	return c.cc.LocalAddr()
}

func (c *conn) RemoteAddr() net.Addr {
	return c.cc.RemoteAddr()
}

func (c *conn) SetDeadline(t time.Time) (err error) {
	return c.cc.SetDeadline(t)
}

func (c *conn) SetReadDeadline(t time.Time) error {
	return c.cc.SetReadDeadline(t)
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	return c.cc.SetWriteDeadline(t)
}
