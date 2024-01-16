package query

import (
	"context"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Session = (*Session)(nil)

type Session struct {
	id          string
	nodeID      int64
	queryClient Ydb_Query_V1.QueryServiceClient
	status      query.SessionStatus
	close       func()
}

func (s *Session) Close(ctx context.Context) error {
	s.close()
	return nil
}

func begin(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID string, txSettings query.TransactionSettings) (*transaction, error) {
	a := allocator.New()
	defer a.Free()
	response, err := client.BeginTransaction(ctx,
		&Ydb_Query.BeginTransactionRequest{
			SessionId:  sessionID,
			TxSettings: txSettings.ToYDB(a),
		},
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(xerrors.Transport(err))
	}
	if response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return nil, xerrors.WithStackTrace(xerrors.FromOperation(response))
	}
	return &transaction{
		id: response.GetTxMeta().GetId(),
	}, nil
}

func (s *Session) Begin(ctx context.Context, txSettings query.TransactionSettings) (query.Transaction, error) {
	tx, err := begin(ctx, s.queryClient, s.id, txSettings)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	tx.s = s
	return tx, nil
}

func (s *Session) ID() string {
	return s.id
}

func (s *Session) NodeID() int64 {
	return s.nodeID
}

func (s *Session) Status() query.SessionStatus {
	status := query.SessionStatus(atomic.LoadUint32((*uint32)(&s.status)))
	return status
}

func (s *Session) Execute(
	ctx context.Context, q string, opts ...query.ExecuteOption,
) (query.Transaction, query.Result, error) {
	return execute(ctx, s, s.queryClient, q, query.ExecuteSettings(opts...))
}
