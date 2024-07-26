package query

import (
	"context"
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ query.Transaction = (*Transaction)(nil)

var ErrTransactionRollingBack = xerrors.Wrap(errors.New("ydb: the transaction is rolling back"))

type Transaction struct {
	tx.Parent

	s           *Session
	onCompleted []OnTransactionCompletedFunc
}

func (tx Transaction) ReadRow(ctx context.Context, q string, opts ...options.TxExecuteOption) (row query.Row, _ error) {
	r, err := tx.Execute(ctx, q, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	defer func() {
		_ = r.Close(ctx)
	}()
	row, err = exactlyOneRowFromResult(ctx, r)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if err = r.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}

func (tx Transaction) ReadResultSet(ctx context.Context, q string, opts ...options.TxExecuteOption) (
	rs query.ResultSet, _ error,
) {
	r, err := tx.Execute(ctx, q, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	defer func() {
		_ = r.Close(ctx)
	}()
	rs, err = exactlyOneResultSetFromResult(ctx, r)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if err = r.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return rs, nil
}

func newTransaction(id string, s *Session) *Transaction {
	return &Transaction{
		Parent: tx.Parent(id),
		s:      s,
	}
}

func (tx Transaction) Execute(ctx context.Context, q string, opts ...options.TxExecuteOption) (
	r query.Result, finalErr error,
) {
	onDone := trace.QueryOnTxExecute(tx.s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.Transaction.Execute"), tx.s, tx, q)
	defer func() {
		onDone(finalErr)
	}()

	executeSettings := options.TxExecuteSettings(tx.ID(), opts...).ExecuteSettings

	var res *result

	// notification about complete transaction must be sended for any error or for successfully read all result if
	// it was execution with commit flag
	defer func() {
		if finalErr == nil {
			go func() {
				<-res.Done()
				resErr := res.Err()
				if resErr != nil || executeSettings.TxControl().IsTxCommit() {
					tx.notifyOnCompleted(resErr)
				}
			}()
		} else {
			tx.notifyOnCompleted(finalErr)
		}
	}()

	var err error
	_, res, err = execute(ctx, tx.s, tx.s.grpcClient, q, executeSettings)
	if err != nil {
		if xerrors.IsOperationError(err) {
			tx.s.setStatus(statusClosed)
		}

		return nil, xerrors.WithStackTrace(err)
	}

	return res, nil
}

func commitTx(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID, txID string) error {
	_, err := client.CommitTransaction(ctx, &Ydb_Query.CommitTransactionRequest{
		SessionId: sessionID,
		TxId:      txID,
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx Transaction) CommitTx(ctx context.Context) (err error) {
	defer func() {
		tx.notifyOnCompleted(err)
	}()

	err = commitTx(ctx, tx.s.grpcClient, tx.s.id, tx.ID())
	if err != nil {
		if xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION) {
			tx.s.setStatus(statusClosed)
		}

		return xerrors.WithStackTrace(err)
	}

	return nil
}

func rollback(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID, txID string) error {
	_, err := client.RollbackTransaction(ctx, &Ydb_Query.RollbackTransactionRequest{
		SessionId: sessionID,
		TxId:      txID,
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx Transaction) Rollback(ctx context.Context) error {
	// ToDo save local marker for deny any additional requests to the transaction?

	tx.notifyOnCompleted(xerrors.WithStackTrace(ErrTransactionRollingBack))

	err := rollback(ctx, tx.s.grpcClient, tx.s.id, tx.ID())
	if err != nil {
		if xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION) {
			tx.s.setStatus(statusClosed)
		}

		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx Transaction) notifyOnCompleted(err error) {
	for _, f := range tx.onCompleted {
		f(err)
	}
}

func GetSessionID(tx *Transaction) string {
	return tx.s.ID()
}

func OnTransactionCompleted(tx *Transaction, f OnTransactionCompletedFunc) {
	tx.onCompleted = append(tx.onCompleted, f)
}

type OnTransactionCompletedFunc func(transactionResult error)
