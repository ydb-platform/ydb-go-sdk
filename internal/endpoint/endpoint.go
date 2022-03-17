package endpoint

import (
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint/info"
)

type Endpoint interface {
	fmt.Stringer

	Info() info.Info
	Copy() Endpoint

	NodeID() uint32
	Address() string
	Location() string
	LocalDC() bool
	LoadFactor() float32
	LastUpdated() time.Time

	Touch(opts ...option)
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
	return fmt.Sprintf(`{id:%d,address:"%s",local:%t,location:"%s",loadFactor:%f,lastUpdated:"%s"}`,
		e.id,
		e.address,
		e.local,
		e.location,
		e.loadFactor,
		e.lastUpdated.Format(time.RFC3339),
	)
}

func (e *endpoint) Info() info.Info {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return info.Info{
		ID:         e.id,
		Address:    e.address,
		LoadFactor: e.loadFactor,
		Local:      e.local,
	}
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

func (e *endpoint) Touch(opts ...option) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, o := range append(
		[]option{
			withLastUpdated(time.Now()),
		},
		opts...,
	) {
		o(e)
	}
}

type option func(e *endpoint)

func WithID(id uint32) option {
	return func(e *endpoint) {
		e.id = id
	}
}

func WithLocation(location string) option {
	return func(e *endpoint) {
		e.location = location
	}
}

func WithLocalDC(local bool) option {
	return func(e *endpoint) {
		e.local = local
	}
}

func WithLoadFactor(loadFactor float32) option {
	return func(e *endpoint) {
		e.loadFactor = loadFactor
	}
}

func WithServices(services []string) option {
	return func(e *endpoint) {
		e.services = append(e.services, services...)
	}
}

func withLastUpdated(ts time.Time) option {
	return func(e *endpoint) {
		e.lastUpdated = ts
	}
}

func New(address string, opts ...option) Endpoint {
	e := &endpoint{
		address:     address,
		lastUpdated: time.Now(),
	}
	for _, o := range opts {
		o(e)
	}
	return e
}
