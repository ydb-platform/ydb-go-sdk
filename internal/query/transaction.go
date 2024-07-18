package query

import (
	"context"

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

type Transaction struct {
	tx.Parent

	s *Session

	beforeCommit []BeforeCommitFunc
	afterCommit  []AfterCommitFunc
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

	_, res, err := execute(ctx, tx.s, tx.s.grpcClient, q, options.TxExecuteSettings(tx.ID(), opts...).ExecuteSettings)
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

func (tx Transaction) CommitTx(ctx context.Context) error {
	err := commitTx(ctx, tx.s.grpcClient, tx.s.id, tx.ID())
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
	err := rollback(ctx, tx.s.grpcClient, tx.s.id, tx.ID())
	if err != nil {
		if xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION) {
			tx.s.setStatus(statusClosed)
		}

		return xerrors.WithStackTrace(err)
	}

	return nil
}

type BeforeCommitFunc func(ctx context.Context) error

// OnBeforeCommit add callback f. The f will be called before sent commit message or tx_commit flag to the server.
// If any of f return error != nil - commit execution will return false and transaction will be rolled back
func OnBeforeCommit(tx *Transaction, f BeforeCommitFunc) {
	if f == nil {
		panic("ydb: callback must be not nil")
	}
	tx.beforeCommit = append(tx.beforeCommit, f)
}

type AfterCommitFunc func(ctx context.Context, commitError error)

// OnAfterCommit add callback f to the tx. The f will be called after commit (successfully or failed) of the tx
// will be completed.
func OnAfterCommit(tx *Transaction, f AfterCommitFunc) {
	if f == nil {
		panic("ydb: callback must be not nil")
	}
	tx.afterCommit = append(tx.afterCommit, f)
}
