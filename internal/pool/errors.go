package pool

import (
	"errors"
)

var (
	errClosedPool     = errors.New("closed pool")
	errItemIsNotAlive = errors.New("item is not alive")
	errPoolIsOverflow = errors.New("pool is overflow")
	errNoProgress     = errors.New("no progress")
)
