package query

import (
	"errors"
)

var (
	ErrNotImplemented          = errors.New("not implemented yet")
	errWrongNextResultSetIndex = errors.New("wrong result set index")
	errNilResult               = errors.New("nil result")
	errClosedResult            = errors.New("result closed early")
	errClosedClient            = errors.New("query client closed early")
	errWrongResultSetIndex     = errors.New("critical violation of the logic - wrong result set index")
)
