package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Transaction = (*transaction)(nil)

type transaction struct {
	id string
	s  *Session
}

func (tx transaction) ID() string {
	return tx.id
}

func fromTxOptions(txID string, txOpts ...query.TxExecuteOption) executeSettings {
	opts := make([]query.ExecuteOption, 0, len(txOpts)+1)
	for _, opt := range txOpts {
		if executeOpt, has := opt.(query.ExecuteOption); has {
			opts = append(opts, executeOpt)
		}
	}
	opts = append(opts, query.WithTxControl(query.TxControl(query.WithTxID(txID))))
	return query.ExecuteSettings(opts...)
}

func (tx transaction) Execute(ctx context.Context, q string, opts ...query.TxExecuteOption) (r query.Result, err error) {
	_, res, err := execute(ctx, tx.s, tx.s.queryClient, q, fromTxOptions(tx.id, opts...))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return res, nil
}

func commitTx(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID string, txID string) error {
	response, err := client.CommitTransaction(ctx, &Ydb_Query.CommitTransactionRequest{
		SessionId: sessionID,
		TxId:      txID,
	})
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Transport(err))
	}
	if response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return xerrors.WithStackTrace(xerrors.FromOperation(response))
	}
	return nil
}

func (tx transaction) CommitTx(ctx context.Context) (err error) {
	return commitTx(ctx, tx.s.queryClient, tx.s.id, tx.id)
}

func rollback(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID string, txID string) error {
	response, err := client.RollbackTransaction(ctx, &Ydb_Query.RollbackTransactionRequest{
		SessionId: sessionID,
		TxId:      txID,
	})
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Transport(err))
	}
	if response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return xerrors.WithStackTrace(xerrors.FromOperation(response))
	}
	return nil
}

func (tx transaction) Rollback(ctx context.Context) (err error) {
	return rollback(ctx, tx.s.queryClient, tx.s.id, tx.id)
}
