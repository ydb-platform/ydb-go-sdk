package query

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrTransactionRollingBack  = xerrors.Wrap(errors.New("ydb: the transaction is rolling back"))
	errWrongNextResultSetIndex = errors.New("wrong result set index")
	errWrongResultSetIndex     = errors.New("critical violation of the logic - wrong result set index")
	errMoreThanOneRow          = errors.New("unexpected more than one row in result set")
	errMoreThanOneResultSet    = errors.New("unexpected more than one result set")
	errNoResultSets            = errors.New("no result sets")
	errNilOption               = errors.New("nil option")
	ErrOptionNotForTxExecute   = errors.New("option is not for execute on transaction")
	errExecuteOnCompletedTx    = errors.New("execute on completed transaction")
)
