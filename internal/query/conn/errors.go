package conn

import "errors"

var errConnClosedEarly = errors.New("Conn closed early")
