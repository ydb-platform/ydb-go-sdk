package query

import (
	"errors"
)

var (
	ErrNotImplemented          = errors.New("not implemented yet")
	errWrongNextResultSetIndex = errors.New("wrong result set index")
	errClosedResult            = errors.New("result closed early")
	errWrongResultSetIndex     = errors.New("critical violation of the logic - wrong result set index")
	errMoreThanOneRow          = errors.New("unexpected more than one row in result set")
	errMoreThanOneResultSet    = errors.New("unexpected more than one result set")
	errNoResultSets            = errors.New("no result sets")
)
