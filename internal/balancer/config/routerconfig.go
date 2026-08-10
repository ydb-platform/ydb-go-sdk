package config

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"

// Compatibility aliases for internal tests and packages while balancer
// strategies are represented by a tree rather than a flat Config.
type (
	Info   = strategy.Info
	Filter = strategy.Filter
)
