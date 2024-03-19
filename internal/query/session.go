package query

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ query.Session = (*Session)(nil)

type (
	Session struct {
		id         string
		nodeID     int64
		grpcClient Ydb_Query_V1.QueryServiceClient
		statusCode statusCode
		trace      *trace.Query
		closeOnce  func(ctx context.Context) error
		checks     []func(s *Session) bool
	}
	sessionOption func(session *Session)
)

func withSessionCheck(f func(*Session) bool) sessionOption {
	return func(s *Session) {
		s.checks = append(s.checks, f)
	}
}

func withSessionTrace(t *trace.Query) sessionOption {
	return func(s *Session) {
		s.trace = s.trace.Compose(t)
	}
}

func createSession(
	ctx context.Context, client Ydb_Query_V1.QueryServiceClient, opts ...sessionOption,
) (s *Session, finalErr error) {
	s = &Session{
		grpcClient: client,
		statusCode: statusUnknown,
		trace:      &trace.Query{},
		checks: []func(*Session) bool{
			func(s *Session) bool {
				switch s.status() {
				case statusIdle, statusInUse:
					return true
				default:
					return false
				}
			},
		},
		closeOnce: xsync.OnceFunc(func(ctx context.Context) (err error) {
			s.setStatus(statusClosing)
			defer s.setStatus(statusClosed)

			if err = deleteSession(ctx, s.grpcClient, s.id); err != nil {
				return xerrors.WithStackTrace(err)
			}

			return nil
		}),
	}
	defer func() {
		if finalErr != nil && s != nil {
			s.setStatus(statusError)
		}
	}()

	for _, opt := range opts {
		opt(s)
	}

	onDone := trace.QueryOnSessionCreate(s.trace, &ctx, stack.FunctionID(""))
	defer func() {
		onDone(s, finalErr)
	}()

	response, err := client.CreateSession(ctx, &Ydb_Query.CreateSessionRequest{})
	if err != nil {
		return nil, xerrors.WithStackTrace(
			xerrors.Transport(err),
		)
	}

	if response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return nil, xerrors.WithStackTrace(
			xerrors.FromOperation(response),
		)
	}

	defer func() {
		if finalErr != nil {
			_ = deleteSession(ctx, client, response.GetSessionId())
		}
	}()

	s.id = response.GetSessionId()
	s.nodeID = response.GetNodeId()

	err = s.attach(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(
			xerrors.Transport(err),
		)
	}

	s.setStatus(statusIdle)

	return s, nil
}

func (s *Session) attach(ctx context.Context) (finalErr error) {
	onDone := trace.QueryOnSessionAttach(s.trace, &ctx, stack.FunctionID(""), s)
	defer func() {
		onDone(finalErr)
	}()

	attach, err := s.grpcClient.AttachSession(xcontext.WithoutDeadline(ctx), &Ydb_Query.AttachSessionRequest{
		SessionId: s.id,
	})
	if err != nil {
		return xerrors.WithStackTrace(
			xerrors.Transport(err),
		)
	}
	state, err := attach.Recv()
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Transport(err))
	}

	if state.GetStatus() != Ydb.StatusIds_SUCCESS {
		return xerrors.WithStackTrace(xerrors.FromOperation(state))
	}

	go func() {
		defer func() {
			_ = s.closeOnce(ctx)
		}()

		for {
			if !s.IsAlive() {
				return
			}
			recv, recvErr := attach.Recv()
			if recvErr != nil {
				if xerrors.Is(recvErr, io.EOF) {
					s.setStatus(statusClosed)
				} else {
					s.setStatus(statusError)
				}

				return
			}
			if recv.GetStatus() != Ydb.StatusIds_SUCCESS {
				s.setStatus(statusError)

				return
			}
		}
	}()

	return nil
}

func deleteSession(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID string) error {
	response, err := client.DeleteSession(ctx,
		&Ydb_Query.DeleteSessionRequest{
			SessionId: sessionID,
		},
	)
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Transport(err))
	}
	if response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return xerrors.WithStackTrace(xerrors.FromOperation(response))
	}

	return nil
}

func (s *Session) IsAlive() bool {
	for _, check := range s.checks {
		if !check(s) {
			return false
		}
	}

	return true
}

func (s *Session) Close(ctx context.Context) (err error) {
	onDone := trace.QueryOnSessionDelete(s.trace, &ctx, stack.FunctionID(""), s)
	defer func() {
		onDone(err)
	}()

	if s.closeOnce != nil {
		return s.closeOnce(ctx)
	}

	return nil
}

func begin(
	ctx context.Context,
	client Ydb_Query_V1.QueryServiceClient,
	sessionID string,
	txSettings query.TransactionSettings,
) (
	*transaction, error,
) {
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

func (s *Session) Begin(
	ctx context.Context,
	txSettings query.TransactionSettings,
) (
	_ query.Transaction, err error,
) {
	var tx *transaction

	onDone := trace.QueryOnSessionBegin(s.trace, &ctx, stack.FunctionID(""), s)
	defer func() {
		onDone(err, tx)
	}()

	tx, err = begin(ctx, s.grpcClient, s.id, txSettings)
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

func (s *Session) status() statusCode {
	return statusCode(atomic.LoadUint32((*uint32)(&s.statusCode)))
}

func (s *Session) setStatus(code statusCode) {
	atomic.StoreUint32((*uint32)(&s.statusCode), uint32(code))
}

func (s *Session) Status() string {
	return s.status().String()
}

func (s *Session) Execute(
	ctx context.Context, q string, opts ...options.ExecuteOption,
) (_ query.Transaction, _ query.Result, err error) {
	onDone := trace.QueryOnSessionExecute(s.trace, &ctx, stack.FunctionID(""), s, q)
	defer func() {
		onDone(err)
	}()

	tx, r, err := execute(ctx, s, s.grpcClient, q, options.ExecuteSettings(opts...))
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}

	return tx, r, nil
}
