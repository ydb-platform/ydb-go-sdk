package ydb

type ConnUsePolicy uint8

const (
	ConnUseDefault ConnUsePolicy = 1 << iota >> 1
	ConnUseBalancer
	ConnUseEndpoint

	ConnUseSmart = ConnUseBalancer | ConnUseEndpoint
)
