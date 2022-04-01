package trace

// tool gtrace used from ./cmd/gtrace

//go:generate gtrace

type (
	// Scheme specified trace of scheme client activity.
	// gtrace:gen
	Scheme struct{}
)
