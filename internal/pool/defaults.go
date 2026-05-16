package pool

import (
	"time"
)

const (
	DefaultLimit         = 50
	defaultCreateTimeout = 500 * time.Millisecond
	defaultCloseTimeout  = 500 * time.Millisecond
)
