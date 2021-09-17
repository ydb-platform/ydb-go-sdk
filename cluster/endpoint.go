package cluster

type Endpoint struct {
	Addr       string
	Port       int
	LoadFactor float32
	Local      bool
}
