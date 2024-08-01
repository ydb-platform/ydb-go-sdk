package tx

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Transaction interface {
	Identifier
	SessionID() string
	OnCompleted(f OnTransactionCompletedFunc)
	Rollback(ctx context.Context) error
}

type OnTransactionCompletedFunc func(transactionResult error)

func AsTransaction(id Identifier) (Transaction, error) {
	if t, ok := id.(Transaction); ok {
		return t, nil
	}

	return nil, xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
		"ydb: waiting ydb transaction object of type query.Transaction, got: %T",
		id,
	)))
}
