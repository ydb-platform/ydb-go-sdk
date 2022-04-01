package trace

// tool gtrace used from ./cmd/gtrace

//go:generate gtrace

type (
	// Coordination specified trace of coordination client activity.
	// gtrace:gen
	Coordination struct{}
)
