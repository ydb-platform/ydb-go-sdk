package mock

import (
	"time"
)

type Endpoint struct {
	AddressField  string
	LocationField string
	NodeIDField   uint32
}

func (e *Endpoint) NodeID() uint32 {
	return e.NodeIDField
}

func (e *Endpoint) Address() string {
	return e.AddressField
}

func (e *Endpoint) Location() string {
	return e.LocationField
}

func (e *Endpoint) LastUpdated() time.Time {
	panic("not implemented in mock")
}

func (e *Endpoint) LoadFactor() float32 {
	panic("not implemented in mock")
}

func (e *Endpoint) String() string {
	panic("not implemented in mock")
}
