package trace

import "context"

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Discovery specified trace of discovery client activity.
	// gtrace:gen
	Discovery struct {
		OnDiscover func(DiscoveryDiscoverStartInfo) func(DiscoveryDiscoverDoneInfo)
		OnWhoAmI   func(DiscoveryWhoAmIStartInfo) func(DiscoveryWhoAmIDoneInfo)
	}
	DiscoveryDiscoverStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Address  string
		Database string
	}
	DiscoveryDiscoverDoneInfo struct {
		Location  string
		Endpoints []EndpointInfo
		Error     error
	}
	DiscoveryWhoAmIStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DiscoveryWhoAmIDoneInfo struct {
		User   string
		Groups []string
		Error  error
	}
)
