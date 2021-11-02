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

func (e Endpoint) Address() string {
	return net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
}

func (e Endpoint) LocalDC() bool {
	return e.Local
}

func New(address string) (e Endpoint, err error) {
	var port string
	e.Host, port, err = net.SplitHostPort(address)
	if err != nil {
		return
	}
	e.Port, err = strconv.Atoi(port)
	return
}
