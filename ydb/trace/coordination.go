package trace

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Coordination specified trace of coordination client activity.
	// gtrace:gen
	Coordination struct{}
)
