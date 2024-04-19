package endpoint

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Info interface {
	NodeID() uint32
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

type Endpoint interface {
	Info

	String() string
	Touch(opts ...Option)
}

type endpoint struct { //nolint:maligned
	id       uint32
	address  string
	location string
	services []string

	loadFactor  atomic.Pointer[float32]
	lastUpdated atomic.Pointer[time.Time]

	local bool
}

func (e *endpoint) String() string {
	return fmt.Sprintf(`{id:%d,address:%q,local:%t,location:%q,loadFactor:%f,lastUpdated:%q}`,
		e.id,
		e.address,
		e.local,
		e.location,
		*e.loadFactor.Load(),
		e.lastUpdated.Load().Format(time.RFC3339),
	)
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

// Deprecated: LocalDC check "local" by compare endpoint location with discovery "selflocation" field.
// It work good only if connection url always point to local dc.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func (e *endpoint) LocalDC() bool {
	return e.local
}

func (e *endpoint) LoadFactor() float32 {
	return *e.loadFactor.Load()
}

func (e *endpoint) LastUpdated() time.Time {
	return *e.lastUpdated.Load()
}

func (e *endpoint) Touch(opts ...Option) {
	for _, opt := range append([]Option{WithLastUpdated(time.Now())}, opts...) {
		if opt != nil {
			opt(e)
		}
	}
}

type Option func(e *endpoint)

func WithLocalDC(local bool) Option {
	return func(e *endpoint) {
		e.local = local
	}
}

func WithLoadFactor(loadFactor float32) Option {
	return func(e *endpoint) {
		e.loadFactor.Store(&loadFactor)
	}
}

func WithServices(services []string) Option {
	return func(e *endpoint) {
		e.services = append(e.services, services...)
	}
}

func WithLastUpdated(ts time.Time) Option {
	return func(e *endpoint) {
		e.lastUpdated.Store(&ts)
	}
}

func New(nodeID uint32, address string, location string, opts ...Option) *endpoint {
	e := &endpoint{
		id:       nodeID,
		address:  address,
		location: location,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}

	return e
}
