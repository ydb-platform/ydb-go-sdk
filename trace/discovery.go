package trace

import "context"

// tool gtrace used from repository github.com/asmyasnikov/cmd/gtrace

//go:generate gtrace

type (
	//gtrace:gen
	//gtrace:set Shortcut
	Discovery struct {
		OnDiscover func(DiscoverStartInfo) func(DiscoverDoneInfo)
		OnWhoAmI   func(WhoAmIStartInfo) func(WhoAmIDoneInfo)
	}
	DiscoverStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Address  string
		Database string
	}
	DiscoverDoneInfo struct {
		Location  string
		Endpoints []string
		Error     error
	}
	WhoAmIStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	WhoAmIDoneInfo struct {
		User   string
		Groups []string
		Error  error
	}
)
