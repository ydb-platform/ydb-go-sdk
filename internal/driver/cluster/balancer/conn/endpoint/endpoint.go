package endpoint

import (
	"net"
	"strconv"
)

type Endpoint struct {
	id      uint32
	address string

	loadFactor float32
	local      bool
}

func (e Endpoint) NodeID() uint32 {
	return e.id
}

func (e Endpoint) Address() string {
	return e.address
}

func (e Endpoint) LocalDC() bool {
	return e.local
}

func (e Endpoint) LoadFactor() float32 {
	return e.loadFactor
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

type option func(e *Endpoint)

func WithID(id uint32) option {
	return func(e *Endpoint) {
		e.id = id
	}
}

func WithLocalDC(local bool) option {
	return func(e *Endpoint) {
		e.local = local
	}
}

func WithLoadFactor(loadFactor float32) option {
	return func(e *Endpoint) {
		e.loadFactor = loadFactor
	}
}

func New(address string, opts ...option) (e Endpoint) {
	e.address = address
	for _, o := range opts {
		o(&e)
	}
	return
}
