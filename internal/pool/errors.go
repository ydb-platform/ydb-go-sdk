package pool

import (
	"errors"
)

var (
	errClosedPool     = errors.New("closed pool")
	errItemIsNotAlive = errors.New("item is not alive")
)
