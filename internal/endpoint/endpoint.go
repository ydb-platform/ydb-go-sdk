package endpoint

import (
	"fmt"
	"sync/atomic"
	"time"
)

type (
	Key struct {
		NodeID  uint32
		Address string
	}
	Info interface {
		fmt.Stringer

		Key() Key
		NodeID() uint32
		Address() string
		Location() string
		LastUpdated() time.Time
		LoadFactor() float32
	}
)

func Equals(rhs, lhs Info) bool {
	if rhs.Address() != lhs.Address() {
		return false
	}
	if rhs.NodeID() != lhs.NodeID() {
		return false
	}
	if rhs.Location() != lhs.Location() {
		return false
	}

	return true
}

type Endpoint interface {
	Info
}

type endpoint struct {
	id       uint32
	address  string
	location string
	services []string

	loadFactor atomic.Pointer[float32]

	lastUpdated atomic.Pointer[time.Time]
}

func (e *endpoint) String() string {
	return fmt.Sprintf(`{id:%d,address:%q,location:%q,loadFactor:%f,lastUpdated:%q}`,
		e.id,
		e.address,
		e.location,
		*e.loadFactor.Load(),
		e.lastUpdated.Load().Format(time.RFC3339),
	)
}

func (e *endpoint) Key() Key {
	return Key{
		NodeID:  e.id,
		Address: e.address,
	}
}

func (e *endpoint) NodeID() uint32 {
	return e.id
}

func (e *endpoint) Address() (address string) {
	return e.address
}

func (e *endpoint) Location() string {
	return e.location
}

func (e *endpoint) LoadFactor() float32 {
	return *e.loadFactor.Load()
}

func (e *endpoint) LastUpdated() time.Time {
	return *e.lastUpdated.Load()
}

type Option func(e *endpoint)

func WithID(id uint32) Option {
	return func(e *endpoint) {
		e.id = id
	}
}

func WithLocation(location string) Option {
	return func(e *endpoint) {
		e.location = location
	}
}

func WithLoadFactor(loadFactor float32) Option {
	return func(e *endpoint) {
		e.loadFactor.Store(&loadFactor)
	}
}

func WithLastUpdated(lastUpdated time.Time) Option {
	return func(e *endpoint) {
		e.lastUpdated.Store(&lastUpdated)
	}
}

func WithServices(services []string) Option {
	return func(e *endpoint) {
		e.services = append(e.services, services...)
	}
}

func New(address string, opts ...Option) *endpoint {
	e := &endpoint{
		address: address,
	}
	t := time.Now()
	e.lastUpdated.Store(&t)
	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}

	return e
}
