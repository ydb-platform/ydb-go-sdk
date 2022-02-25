package endpoint

import (
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint/info"
)

type Endpoint interface {
	Info() info.Info
	String() string

	NodeID() uint32
	Address() string
	Location() string
	LocalDC() bool
	LoadFactor() float32
	LastUpdated() time.Time

	Touch(opts ...option)
}

type endpoint struct {
	id       uint32
	address  string
	location string
	services []string

	loadFactor float32
	local      bool

	lastUpdated time.Time
}

func (e *endpoint) String() string {
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
	return info.Info{
		ID:         e.id,
		Address:    e.address,
		LoadFactor: e.loadFactor,
		Local:      e.local,
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

func (e *endpoint) LocalDC() bool {
	return e.local
}

func (e *endpoint) LoadFactor() float32 {
	return e.loadFactor
}

func (e *endpoint) LastUpdated() time.Time {
	return e.lastUpdated
}

func (e *endpoint) Touch(opts ...option) {
	e.lastUpdated = time.Now()
	for _, o := range opts {
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

func WithLastUpdated(ts time.Time) option {
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
