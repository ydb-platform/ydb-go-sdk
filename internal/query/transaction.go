package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ query.Transaction = (*transaction)(nil)

type transaction struct {
	id        string
	sessionID string

	client Ydb_Query_V1.QueryServiceClient
	trace  *trace.Query
}

func newTx(txID, sessionID string, client Ydb_Query_V1.QueryServiceClient, trace *trace.Query) *transaction {
	return &transaction{
		id:        txID,
		sessionID: sessionID,
		client:    client,
		trace:     trace,
	}
}

func (tx transaction) ID() string {
	return tx.id
}

func (tx transaction) Execute(ctx context.Context, q string, opts ...options.TxExecuteOption) (
	r query.Result, finalErr error,
) {
	onDone := trace.QueryOnTxExecute(tx.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.transaction.Execute"), tx, q)
	defer func() {
		onDone(finalErr)
	}()

	a := allocator.New()
	defer a.Free()

	settings := options.TxExecuteSettings(tx.id, opts...)

	_, res, err := Execute(ctx, tx.client, tx.sessionID, q, settings.ExecuteOptions...)
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
	return commitTx(ctx, tx.client, tx.sessionID, tx.id)
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

func (tx transaction) Rollback(ctx context.Context) (err error) {
	return rollback(ctx, tx.client, tx.sessionID, tx.id)
}
