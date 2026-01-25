package query

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var (
	ErrMoreThanOneRow         = query.ErrMoreThanOneRow
	ErrNoRows                 = query.ErrNoRows
	ErrMoreThanOneResultSet   = query.ErrMoreThanOneResultSet
	ErrNoResultSets           = query.ErrNoResultSets
	ErrOptionNotForTxExecute  = query.ErrOptionNotForTxExecute
	ErrTransactionRollingBack = xerrors.Wrap(errors.New("the transaction is rolling back"))

	errNilClient               = xerrors.Wrap(errors.New("table client is not initialized"))
	errWrongNextResultSetIndex = errors.New("wrong result set index")
	errWrongResultSetIndex     = errors.New("critical violation of the logic - wrong result set index")
	errNilOption               = errors.New("nil option")
	errExecuteOnCompletedTx    = errors.New("execute on completed transaction")
	errSessionClosed           = errors.New("session is closed")
)
