package ydb

// Deprecated: no need to use connection preferring policy
type ConnUsePolicy uint8

const (
	ConnUseDefault ConnUsePolicy = 1 << iota >> 1
	ConnUseBalancer
	ConnUseEndpoint

	ConnUseSmart = ConnUseBalancer | ConnUseEndpoint
)
