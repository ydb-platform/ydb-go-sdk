package ydb

import (
	"errors"
)

var (
	// DefaultDiscoveryInterval contains default duration between discovery
	// requests made by driver.

	// DefaultBalancingMethod contains driver's default balancing algorithm.

	// ErrClosed is returned when operation requested on a closed driver.
	ErrClosed = errors.New("driver closed")

	// ErrNilConnection is returned when use nil preferred connection
	ErrNilConnection = errors.New("nil connection")
)
