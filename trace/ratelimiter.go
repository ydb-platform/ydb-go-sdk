package trace

// tool gtrace used from ./cmd/gtrace

//go:generate gtrace

type (
	// Ratelimiter specified trace of ratelimiter client activity.
	// gtrace:gen
	Ratelimiter struct{}
)
