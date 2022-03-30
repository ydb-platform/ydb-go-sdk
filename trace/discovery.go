package trace

import "context"

// tool gtrace used from ./cmd/gtrace

//go:generate gtrace

type (
	//gtrace:gen
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
	}
	DiscoveryWhoAmIDoneInfo struct {
		User   string
		Groups []string
		Error  error
	}
)
