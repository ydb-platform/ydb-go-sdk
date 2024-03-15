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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Session = (*Session)(nil)

type (
	Session struct {
		id         string
		nodeID     int64
		grpcClient Ydb_Query_V1.QueryServiceClient
		statusCode statusCode
		trace      *traceSession
		closeOnce  func(ctx context.Context) error
	}
	traceSession struct {
		onCreate func(ctx context.Context, functionID stack.Caller) func(*Session, error)
		onAttach func(ctx context.Context, functionID stack.Caller, s *Session) func(err error)
		onClose  func(ctx context.Context, functionID stack.Caller, s *Session) func(err error)
	}
	sessionOption func(session *Session)
)

func withSessionTrace(t *traceSession) sessionOption {
	return func(s *Session) {
		if t.onCreate != nil {
			h1 := s.trace.onCreate
			h2 := t.onCreate
			s.trace.onCreate = func(ctx context.Context, functionID stack.Caller) func(*Session, error) {
				var r, r1 func(*Session, error)
				if h1 != nil {
					r = h1(ctx, functionID)
				}
				if h2 != nil {
					r1 = h2(ctx, functionID)
				}

				return func(session *Session, err error) {
					if r != nil {
						r(session, err)
					}
					if r1 != nil {
						r1(session, err)
					}
				}
			}
		}
		if t.onAttach != nil {
			h1 := s.trace.onAttach
			h2 := t.onAttach
			s.trace.onAttach = func(ctx context.Context, functionID stack.Caller, session *Session) func(error) {
				var r, r1 func(error)
				if h1 != nil {
					r = h1(ctx, functionID, session)
				}
				if h2 != nil {
					r1 = h2(ctx, functionID, session)
				}

				return func(err error) {
					if r != nil {
						r(err)
					}
					if r1 != nil {
						r1(err)
					}
				}
			}
		}
		if t.onClose != nil {
			h1 := s.trace.onClose
			h2 := t.onClose
			s.trace.onClose = func(ctx context.Context, functionID stack.Caller, session *Session) func(error) {
				var r, r1 func(error)
				if h1 != nil {
					r = h1(ctx, functionID, session)
				}
				if h2 != nil {
					r1 = h2(ctx, functionID, session)
				}

				return func(err error) {
					if r != nil {
						r(err)
					}
					if r1 != nil {
						r1(err)
					}
				}
			}
		}
	}
}

func createSession(
	ctx context.Context, client Ydb_Query_V1.QueryServiceClient, opts ...sessionOption,
) (_ *Session, finalErr error) {
	s := &Session{
		grpcClient: client,
		statusCode: statusUnknown,
		trace:      defaultSessionTrace,
	}
	defer func() {
		if finalErr != nil {
			s.setStatus(statusError)
		}
	}()

	for _, opt := range opts {
		opt(s)
	}

	onDone := s.trace.onCreate(ctx, stack.FunctionID(""))
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
	onDone := s.trace.onAttach(ctx, stack.FunctionID(""), s)
	defer func() {
		onDone(finalErr)
	}()

	attach, err := s.grpcClient.AttachSession(context.Background(), &Ydb_Query.AttachSessionRequest{
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

	s.closeOnce = xsync.OnceFunc(func(ctx context.Context) (err error) {
		s.setStatus(statusClosing)
		defer s.setStatus(statusClosed)

		if s.trace.onClose != nil {
			onClose := s.trace.onClose(ctx, stack.FunctionID(""), s)
			defer func() {
				onClose(err)
			}()
		}

		if err = deleteSession(ctx, s.grpcClient, s.id); err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	})

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
	switch s.status() {
	case statusIdle, statusInUse:
		return true
	default:
		return false
	}
}

func (s *Session) Close(ctx context.Context) error {
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
	query.Transaction, error,
) {
	tx, err := begin(ctx, s.grpcClient, s.id, txSettings)
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
) (query.Transaction, query.Result, error) {
	tx, r, err := execute(ctx, s, s.grpcClient, q, options.ExecuteSettings(opts...))
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}

	return tx, r, nil
}
