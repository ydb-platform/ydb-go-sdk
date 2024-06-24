package pool

import (
	"errors"
)

var (
	errClosedPool = errors.New("closed pool")
)
