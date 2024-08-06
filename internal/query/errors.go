package query

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrTransactionRollingBack  = xerrors.Wrap(errors.New("ydb: the transaction is rolling back"))
	ErrNotImplemented          = errors.New("not implemented yet")
	errWrongNextResultSetIndex = errors.New("wrong result set index")
	errClosedResult            = errors.New("result closed early")
	errWrongResultSetIndex     = errors.New("critical violation of the logic - wrong result set index")
	errMoreThanOneRow          = errors.New("unexpected more than one row in result set")
	errMoreThanOneResultSet    = errors.New("unexpected more than one result set")
	errNoResultSets            = errors.New("no result sets")
)
