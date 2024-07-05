package endpoint

import (
	"fmt"
	"sync"
	"time"
)

type (
	NodeID interface {
		NodeID() uint32
	}
	Info interface {
		NodeID
		Address() string
		Location() string
		LastUpdated() time.Time
		LoadFactor() float32

		// Deprecated: LocalDC check "local" by compare endpoint location with discovery "selflocation" field.
		// It work good only if connection url always point to local dc.
		// Will be removed after Oct 2024.
		// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
		LocalDC() bool
	}
	Endpoint interface {
		Info

		String() string
		Copy() Endpoint
		Touch(opts ...Option)
	}
)

type endpoint struct { //nolint:maligned
	mu       sync.RWMutex
	id       uint32
	address  string
	location string
	services []string

	loadFactor  float32
	lastUpdated time.Time

	local bool
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

// Deprecated: LocalDC check "local" by compare endpoint location with discovery "selflocation" field.
// It work good only if connection url always point to local dc.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
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
	for _, opt := range append([]Option{WithLastUpdated(time.Now())}, opts...) {
		if opt != nil {
			opt(e)
		}
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

func WithLastUpdated(ts time.Time) Option {
	return func(e *endpoint) {
		e.lastUpdated = ts
	}
}

func New(address string, opts ...Option) *endpoint {
	e := &endpoint{
		address:     address,
		lastUpdated: time.Now(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}

	return e
}
