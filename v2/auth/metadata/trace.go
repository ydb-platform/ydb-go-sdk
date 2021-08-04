package metadata

import (
	"net"
	"time"
)

type ClientTrace struct {
	DialStart         func(DialStartInfo)
	DialDone          func(DialDoneInfo)
	WriteRequestStart func(WriteRequestStartInfo)
	WriteRequestDone  func(WriteRequestDoneInfo)
	ReadResponseStart func(ReadResponseStartInfo)
	ReadResponseDone  func(ReadResponseDoneInfo)
}

type (
	DialStartInfo struct {
		Network string
		Addr    string
	}
	DialDoneInfo struct {
		Network string
		Addr    string
		Conn    net.Conn
		Error   error
	}
	WriteRequestStartInfo struct {
		Conn net.Conn
	}
	WriteRequestDoneInfo struct {
		Conn  net.Conn
		Error error
	}
	ReadResponseStartInfo struct {
		Conn net.Conn
	}
	ReadResponseDoneInfo struct {
		Conn    net.Conn
		Code    string
		Expires time.Time
		Error   error
	}
)

func (c *Client) traceDialStart(network, addr string) {
	if c.Trace.DialStart == nil {
		return
	}
	c.Trace.DialStart(DialStartInfo{
		Network: network,
		Addr:    addr,
	})
}
func (c *Client) traceDialDone(network, addr string, conn net.Conn, err error) {
	if c.Trace.DialDone == nil {
		return
	}
	c.Trace.DialDone(DialDoneInfo{
		Network: network,
		Addr:    addr,
		Conn:    conn,
		Error:   err,
	})
}
func (c *Client) traceWriteRequestStart(conn net.Conn) {
	if c.Trace.WriteRequestStart == nil {
		return
	}
	c.Trace.WriteRequestStart(WriteRequestStartInfo{
		Conn: conn,
	})
}
func (c *Client) traceWriteRequestDone(conn net.Conn, err error) {
	if c.Trace.WriteRequestDone == nil {
		return
	}
	c.Trace.WriteRequestDone(WriteRequestDoneInfo{
		Conn:  conn,
		Error: err,
	})
}
func (c *Client) traceReadResponseStart(conn net.Conn) {
	if c.Trace.ReadResponseStart == nil {
		return
	}
	c.Trace.ReadResponseStart(ReadResponseStartInfo{
		Conn: conn,
	})
}
func (c *Client) traceReadResponseDone(
	conn net.Conn, code string, expires time.Time, err error,
) {
	if c.Trace.ReadResponseDone == nil {
		return
	}
	c.Trace.ReadResponseDone(ReadResponseDoneInfo{
		Conn:    conn,
		Code:    code,
		Expires: expires,
		Error:   err,
	})
}
