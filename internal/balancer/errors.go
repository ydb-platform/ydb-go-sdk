package balancer

import (
	"errors"
)

var (
	errEndpointNotDiscovered = errors.New("endpoint is no longer discovered")
)
