package endpoint

type Endpoint struct {
	Addr

	LoadFactor float32
	Local      bool
}

func (e Endpoint) Address() string {
	return e.Addr.String()
}
