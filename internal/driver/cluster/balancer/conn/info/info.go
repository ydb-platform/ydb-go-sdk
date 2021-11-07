package info

// Info contains connection "static" stats – e.g. such that obtained from
// discovery routine.
type Info struct {
	ID         uint32
	LoadFactor float32
	Local      bool
}
