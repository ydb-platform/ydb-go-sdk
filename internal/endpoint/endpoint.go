package endpoint

import (
	"fmt"
	"sync"
	"time"
)

type Info interface {
	NodeID() uint32
	Address() string
	LocalDC() bool
	Location() string
	LastUpdated() time.Time
	LoadFactor() float32
}

type Endpoint interface {
	Info

	String() string
	Copy() Endpoint
	Touch(opts ...Option)
}

type endpoint struct {
	mu       sync.RWMutex
	id       uint32
	address  string
	location string
	services []string

	loadFactor float32
	local      bool

	lastUpdated time.Time
}

func (e *endpoint) Copy() Endpoint {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return &endpoint{
		id:          e.id,
		address:     e.address,
		location:    e.location,
		services:    append(make([]string, 0, len(e.services)), e.services...),
		loadFactor:  e.loadFactor,
		local:       e.local,
		lastUpdated: e.lastUpdated,
	}
}

func (e *endpoint) String() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return fmt.Sprintf(`{id:%d,address:%q,local:%t,location:%q,loadFactor:%f,lastUpdated:%q}`,
		e.id,
		e.address,
		e.local,
		e.location,
		e.loadFactor,
		e.lastUpdated.Format(time.RFC3339),
	)
}

func (e *endpoint) NodeID() uint32 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.id
}

func (e *endpoint) Address() (address string) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.address
}

func (e *endpoint) Location() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.location
}

func (e *endpoint) LocalDC() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.local
}

func (e *endpoint) LoadFactor() float32 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.loadFactor
}

func (e *endpoint) LastUpdated() time.Time {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastUpdated
}

func (e *endpoint) Touch(opts ...Option) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, o := range append(
		[]Option{
			withLastUpdated(time.Now()),
		},
		opts...,
	) {
		o(e)
	}
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

func WithLocalDC(local bool) Option {
	return func(e *endpoint) {
		e.local = local
	}
}

func WithLoadFactor(loadFactor float32) Option {
	return func(e *endpoint) {
		e.loadFactor = loadFactor
	}
}

func WithServices(services []string) Option {
	return func(e *endpoint) {
		e.services = append(e.services, services...)
	}
}

func withLastUpdated(ts time.Time) Option {
	return func(e *endpoint) {
		e.lastUpdated = ts
	}
}

func New(address string, opts ...Option) *endpoint {
	e := &endpoint{
		address:     address,
		lastUpdated: time.Now(),
	}
	for _, o := range opts {
		if o != nil {
			o(e)
		}
	}
	return e
}
