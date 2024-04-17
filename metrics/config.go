package metrics

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

// Config is interface for metrics registry config
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
type Config interface {
	Registry

	// Details returns bitmask for customize details of NewScope
	// If zero - use full set of driver NewScope
	Details() trace.Details

	// WithSystem returns new Config with subsystem scope
	// Separator for split scopes of NewScope provided Config implementation
	WithSystem(subsystem string) Config
}
