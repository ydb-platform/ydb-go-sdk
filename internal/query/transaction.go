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

var _ query.Transaction = (*transaction)(nil)

type transaction struct {
	tx.Parent

	s *Session
}

func (tx transaction) ReadRow(ctx context.Context, q string, opts ...options.TxExecuteOption) (row query.Row, _ error) {
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

func (tx transaction) ReadResultSet(ctx context.Context, q string, opts ...options.TxExecuteOption) (
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

func newTransaction(id string, s *Session) *transaction {
	return &transaction{
		Parent: tx.Parent(id),
		s:      s,
	}
}

func (tx transaction) Execute(ctx context.Context, q string, opts ...options.TxExecuteOption) (
	r query.Result, finalErr error,
) {
	onDone := trace.QueryOnTxExecute(tx.s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.transaction.Execute"), tx.s, tx, q)
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

func (tx transaction) CommitTx(ctx context.Context) error {
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

func (tx transaction) Rollback(ctx context.Context) error {
	err := rollback(ctx, tx.s.grpcClient, tx.s.id, tx.ID())
	if err != nil {
		if xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION) {
			tx.s.setStatus(statusClosed)
		}

		return xerrors.WithStackTrace(err)
	}

	return nil
}
