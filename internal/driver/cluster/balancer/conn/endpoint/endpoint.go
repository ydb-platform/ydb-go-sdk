package endpoint

import (
	"net"
	"strconv"
)

type Endpoint struct {
	ID   uint32
	Host string
	Port int

	LoadFactor float32
	Local      bool
}

func (e Endpoint) NodeID() uint32 {
	return e.ID
}

func (e Endpoint) Address() string {
	return net.JoinHostPort(e.Host, strconv.FormatUint(uint64(e.Port), 10))
}

func (e Endpoint) LocalDC() bool {
	return e.Local
}

func SplitHostPort(addr string) (host string, port int, err error) {
	var prt string
	host, prt, err = net.SplitHostPort(addr)
	if err != nil {
		return
	}
	port, err = strconv.Atoi(prt)
	return
}

func New(address string) (e Endpoint, err error) {
	e.Host, e.Port, err = SplitHostPort(address)
	return
}
