package pool

import (
	"time"
)

const (
	DefaultLimit = 50
	// Short create/close timeouts surface unreachable YDB nodes faster (CreateSession /
	// DeleteSession fail quickly), so retry kicks in earlier and the call succeeds sooner
	// instead of waiting on multi-second defaults during cold start or overload.
	defaultCreateTimeout = 500 * time.Millisecond
	defaultCloseTimeout  = 500 * time.Millisecond
)
