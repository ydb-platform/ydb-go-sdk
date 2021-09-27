package endpoint

import (
	"net"
	"strconv"
)

type Addr struct {
	Host string
	Port int
}

func (c Addr) String() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

func String(host string, port int) string {
	return Addr{Host: host, Port: port}.String()
}
