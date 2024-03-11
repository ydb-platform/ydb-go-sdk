package pool

import (
	"errors"
)

var (
	errClosedPool   = errors.New("closed Pool")
	errPoolOverflow = errors.New("Pool overflow")
)
