package query

import (
	"errors"
)

var (
	ErrNotImplemented             = errors.New("not implemented yet")
	errRedundantCallNextResultSet = errors.New("redundant call NextResultSet()")
	errWrongNextResultSetIndex    = errors.New("wrong result set index")
	errInterruptedStream          = errors.New("interrupted stream")
	errClosedResult               = errors.New("result closed early")
	errWrongResultSetIndex        = errors.New("critical violation of the logic - wrong result set index")
	errWrongArgumentsCount        = errors.New("wrong arguments count")
)
