package ydb

import (
	"net"
	"strconv"
)

type connAddr struct {
	addr string
	port int
}

func (c connAddr) String() string {
	return net.JoinHostPort(c.addr, strconv.Itoa(c.port))
}
