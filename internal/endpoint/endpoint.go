package endpoint

type Endpoint struct {
	id       uint32
	address  string
	location string
	services []string

	loadFactor float32
	local      bool
}

func (e Endpoint) NodeID() uint32 {
	return e.id
}

func (e Endpoint) Address() (address string) {
	return e.address
}

func (e Endpoint) Location() string {
	return e.location
}

func (e Endpoint) LocalDC() bool {
	return e.local
}

func (e Endpoint) LoadFactor() float32 {
	return e.loadFactor
}

type option func(e *Endpoint)

func WithID(id uint32) option {
	return func(e *Endpoint) {
		e.id = id
	}
}

func WithLocation(location string) option {
	return func(e *Endpoint) {
		e.location = location
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

func WithServices(services []string) option {
	return func(e *Endpoint) {
		e.services = append(e.services, services...)
	}
}

func New(address string, opts ...option) (e Endpoint) {
	e.address = address
	for _, o := range opts {
		o(&e)
	}
	return
}
