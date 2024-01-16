package query

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Session = (*session)(nil)

type session struct {
	id           string
	nodeID       int64
	queryService Ydb_Query_V1.QueryServiceClient
	status       query.SessionStatus
	lastUsage    atomic.Pointer[time.Time]
}

func (s *session) Begin(ctx context.Context, txSettings *query.TransactionSettings) (query.Transaction, error) {
	response, err := s.queryService.BeginTransaction(ctx,
		&Ydb_Query.BeginTransactionRequest{
			SessionId:  s.id,
			TxSettings: txSettings.Desc(),
		},
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(xerrors.Transport(err))
	}
	if response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return nil, xerrors.WithStackTrace(xerrors.Operation(xerrors.FromOperation(response)))
	}
	return transaction{
		id: response.GetTxMeta().GetId(),
	}, nil
}

func (s *session) ID() string {
	return s.id
}

func (s *session) NodeID() int64 {
	return s.nodeID
}

func (s *session) Status() query.SessionStatus {
	status := query.SessionStatus(atomic.LoadUint32((*uint32)(&s.status)))
	return status
}

func (s *session) LastUsage() time.Time {
	return *s.lastUsage.Load()
}

func (s *session) updateLastUsage() {
	t := time.Now()
	s.lastUsage.Store(&t)
}

func queryFromText(q string, syntax Ydb_Query.Syntax) *Ydb_Query.QueryContent {
	return &Ydb_Query.QueryContent{
		Syntax: syntax,
		Text:   q,
	}
}

func (s *session) Execute(
	ctx context.Context, q string, opts ...query.ExecuteOption,
) (
	txr query.Transaction,
	r query.Result,
	err error,
) {
	var (
		executeOpts = query.NewExecuteOptions(opts...)
		a           = allocator.New()
		request     = Ydb_Query.ExecuteQueryRequest{
			SessionId: s.id,
			ExecMode:  0,
			TxControl: executeOpts.TxControl().Desc(),
			Query: &Ydb_Query.ExecuteQueryRequest_QueryContent{
				QueryContent: queryFromText(q, executeOpts.Syntax()),
			},
			Parameters:           executeOpts.Params().ToYDB(a),
			StatsMode:            0,
			ConcurrentResultSets: false,
		}
		//stream Ydb_Query_V1.QueryService_ExecuteQueryClient
	)
	defer func() {
		a.Free()
	}()

	ctx, cancel := xcontext.WithCancel(ctx)

	_, err = s.queryService.ExecuteQuery(ctx, &request, executeOpts.CallOptions()...)
	if err != nil {
		cancel()
		return nil, nil, xerrors.WithStackTrace(err)
	}
	return nil, nil, xerrors.WithStackTrace(err)
	//
	//return scanner.NewStream(ctx,
	//	func(ctx context.Context) (
	//		set *Ydb.ResultSet,
	//		stats *Ydb_TableStats.QueryStats,
	//		err error,
	//	) {
	//		defer func() {
	//			onIntermediate(xerrors.HideEOF(err))
	//		}()
	//		select {
	//		case <-ctx.Done():
	//			return nil, nil, xerrors.WithStackTrace(ctx.Err())
	//		default:
	//			var response *Ydb_Table.ExecuteScanQueryPartialResponse
	//			response, err = stream.Recv()
	//			result := response.GetResult()
	//			if result == nil || err != nil {
	//				return nil, nil, xerrors.WithStackTrace(err)
	//			}
	//			return result.GetResultSet(), result.GetQueryStats(), nil
	//		}
	//	},
	//	func(err error) error {
	//		cancel()
	//		onIntermediate(xerrors.HideEOF(err))(xerrors.HideEOF(err))
	//		return err
	//	},
	//	scanner.WithIgnoreTruncated(s.config.IgnoreTruncated()),
	//	scanner.WithMarkTruncatedAsRetryable(),
	//)
	//
}
