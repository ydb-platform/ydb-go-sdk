package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ query.Transaction = (*transaction)(nil)

type transaction struct {
	id    string
	s     *Session
	trace *trace.Query
}

func newTransaction(id string, s *Session, t *trace.Query) *transaction {
	if t == nil {
		t = &trace.Query{}
	}

	return &transaction{
		id:    id,
		s:     s,
		trace: t,
	}
}

func (tx transaction) ID() string {
	return tx.id
}

func (tx transaction) Execute(ctx context.Context, q string, opts ...options.TxExecuteOption) (
	r query.Result, finalErr error,
) {
	onDone := trace.QueryOnTxExecute(tx.trace, &ctx, stack.FunctionID(""), tx.s, tx, q)
	defer func() {
		onDone(finalErr)
	}()

	_, res, err := execute(ctx, tx.s, tx.s.grpcClient, q, options.TxExecuteSettings(tx.id, opts...).ExecuteSettings)
	if err != nil {
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

func (tx transaction) CommitTx(ctx context.Context) (err error) {
	return commitTx(ctx, tx.s.grpcClient, tx.s.id, tx.id)
}

func rollback(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID, txID string) error {
	response, err := client.RollbackTransaction(ctx, &Ydb_Query.RollbackTransactionRequest{
		SessionId: sessionID,
		TxId:      txID,
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	if response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return xerrors.WithStackTrace(xerrors.FromOperation(response))
	}

	return nil
}

func (tx transaction) Rollback(ctx context.Context) (err error) {
	return rollback(ctx, tx.s.grpcClient, tx.s.id, tx.id)
}
