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

	errNilPool                 = xerrors.Wrap(errors.New("query client required external session pool"))
	errNilClient               = xerrors.Wrap(errors.New("table client is not initialized"))
	errClosedClient            = xerrors.Wrap(errors.New("query client closed early"))
	errWrongNextResultSetIndex = errors.New("wrong result set index")
	errWrongResultSetIndex     = errors.New("critical violation of the logic - wrong result set index")
	errNilOption               = errors.New("nil option")
	errExecuteOnCompletedTx    = errors.New("execute on completed transaction")
	errSessionClosed           = errors.New("session is closed")
	errNodeShutdownHint        = xerrors.Wrap(errors.New("received node shutdown hint"))
	errSessionShutdownHint     = xerrors.Wrap(errors.New("received session shutdown hint"))
)
