package endpoint

type Endpoint struct {
	Addr

	LoadFactor float32
	Local      bool
}
