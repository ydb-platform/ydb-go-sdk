package trace

// tool gtrace used from ./cmd/gtrace

//go:generate gtrace

type (
	//gtrace:gen
	Ratelimiter struct{}
)
