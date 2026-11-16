package trace

// SessionInfo contains session metadata for trace hooks.
type SessionInfo interface {
	ID() string
	NodeID() uint32
	Status() string
}
