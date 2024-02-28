package trace

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Query specified trace of retry call activity.
	// gtrace:gen
	Query struct{}
)
