package trace

// tool gtrace used from repository github.com/asmyasnikov/cmd/gtrace

//go:generate gtrace

type (
	//gtrace:gen
	//gtrace:set Shortcut
	Ratelimiter struct{}
)
