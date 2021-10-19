package dial

import (
	"net"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type netConn struct {
	address string
	trace   trace.Driver
	raw     net.Conn
}

func (c *netConn) Read(b []byte) (n int, err error) {
	onDone := trace.DriverOnConnReceiveBytes(c.trace, c.address, len(b))
	defer func() {
		onDone(n, err)
	}()
	return c.raw.Read(b)
}

func (c *netConn) Write(b []byte) (n int, err error) {
	onDone := trace.DriverOnConnSendBytes(c.trace, c.address, len(b))
	defer func() {
		onDone(n, err)
	}()
	return c.raw.Write(b)
}

func (c *netConn) Close() error {
	return c.raw.Close()
}

func (c *netConn) LocalAddr() net.Addr {
	return c.raw.LocalAddr()
}

func (c *netConn) RemoteAddr() net.Addr {
	return c.raw.RemoteAddr()
}

func (c *netConn) SetDeadline(t time.Time) error {
	return c.raw.SetDeadline(t)
}

func (c *netConn) SetReadDeadline(t time.Time) error {
	return c.raw.SetReadDeadline(t)
}

func (c *netConn) SetWriteDeadline(t time.Time) error {
	return c.raw.SetWriteDeadline(t)
}
