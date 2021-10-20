package endpoint

import (
	"net"
	"strconv"
)

type NodeID uint32

type Endpoint struct {
	Addr

	ID         NodeID
	LoadFactor float32
	Local      bool
}

func (e Endpoint) NodeID() NodeID {
	return e.ID
}

func (e Endpoint) Address() string {
	return e.Addr.String()
}

func (e Endpoint) LocalDC() bool {
	return e.Local
}

func New(address string) (e Endpoint, err error) {
	var port string
	e.Addr.Host, port, err = net.SplitHostPort(address)
	if err != nil {
		return
	}
	e.Addr.Port, err = strconv.Atoi(port)
	return
}
