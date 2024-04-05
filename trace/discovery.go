package trace

import "context"

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Discovery specified trace of discovery client activity.
	// gtrace:gen
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	Discovery struct {
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnDiscover func(DiscoveryDiscoverStartInfo) func(DiscoveryDiscoverDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnWhoAmI func(DiscoveryWhoAmIStartInfo) func(DiscoveryWhoAmIDoneInfo)
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
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
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DiscoveryDiscoverDoneInfo struct {
		Location  string
		Endpoints []EndpointInfo
		Error     error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
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
