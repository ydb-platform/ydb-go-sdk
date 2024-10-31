package conn

import "errors"

var (
	errConnClosedEarly = errors.New("conn closed early")
)
