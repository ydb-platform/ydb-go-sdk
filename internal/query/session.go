package query

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
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
		cfg        *config.Config
		id         string
		nodeID     int64
		grpcClient Ydb_Query_V1.QueryServiceClient
		statusCode statusCode
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

func createSession(
	ctx context.Context, client Ydb_Query_V1.QueryServiceClient, cfg *config.Config, opts ...sessionOption,
) (s *Session, finalErr error) {
	s = &Session{
		cfg:        cfg,
		id:         "",
		nodeID:     0,
		grpcClient: client,
		statusCode: statusUnknown,
		closeOnce:  nil,
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
	}
	defer func() {
		if finalErr != nil && s != nil {
			s.setStatus(statusError)
		}
	}()

	for _, opt := range opts {
		opt(s)
	}

	onDone := trace.QueryOnSessionCreate(s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.createSession"),
	)
	defer func() {
		onDone(s, finalErr)
	}()

	response, err := client.CreateSession(ctx, &Ydb_Query.CreateSessionRequest{})
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
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
		return nil, xerrors.WithStackTrace(err)
	}

	s.setStatus(statusIdle)

	return s, nil
}

func (s *Session) attach(ctx context.Context) (finalErr error) {
	onDone := trace.QueryOnSessionAttach(s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.(*Session).attach"), s)
	defer func() {
		onDone(finalErr)
	}()

	attachCtx, cancelAttach := xcontext.WithCancel(xcontext.ValueOnly(ctx))

	attach, err := s.grpcClient.AttachSession(attachCtx, &Ydb_Query.AttachSessionRequest{
		SessionId: s.id,
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	_, err = attach.Recv()
	if err != nil {
		cancelAttach()

		return xerrors.WithStackTrace(err)
	}

	s.closeOnce = xsync.OnceFunc(s.closeAndDeleteSession(cancelAttach))

	go func() {
		defer func() {
			_ = s.closeOnce(xcontext.ValueOnly(ctx))
		}()

		for {
			if !s.IsAlive() {
				return
			}
			_, recvErr := attach.Recv()
			if recvErr != nil {
				if xerrors.Is(recvErr, io.EOF) {
					s.setStatus(statusClosed)
				} else {
					s.setStatus(statusError)
				}

				return
			}
		}
	}()

	return nil
}

func (s *Session) closeAndDeleteSession(cancelAttach context.CancelFunc) func(ctx context.Context) (err error) {
	return func(ctx context.Context) (err error) {
		defer cancelAttach()

		s.setStatus(statusClosing)
		defer s.setStatus(statusClosed)

		var cancel context.CancelFunc
		if d := s.cfg.SessionDeleteTimeout(); d > 0 {
			ctx, cancel = xcontext.WithTimeout(ctx, d)
		} else {
			ctx, cancel = xcontext.WithCancel(ctx)
		}
		defer cancel()

		if err = deleteSession(ctx, s.grpcClient, s.id); err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}
}

func deleteSession(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID string) error {
	_, err := client.DeleteSession(ctx,
		&Ydb_Query.DeleteSessionRequest{
			SessionId: sessionID,
		},
	)
	if err != nil {
		return xerrors.WithStackTrace(err)
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
	onDone := trace.QueryOnSessionDelete(s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.(*Session).Close"), s)
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
	s *Session,
	txSettings query.TransactionSettings,
) (*transaction, error) {
	a := allocator.New()
	defer a.Free()
	response, err := client.BeginTransaction(ctx,
		&Ydb_Query.BeginTransactionRequest{
			SessionId:  s.id,
			TxSettings: txSettings.ToYDB(a),
		},
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return newTransaction(response.GetTxMeta().GetId(), s), nil
}

func (s *Session) Begin(
	ctx context.Context,
	txSettings query.TransactionSettings,
) (
	_ query.Transaction, err error,
) {
	var tx *transaction

	onDone := trace.QueryOnSessionBegin(s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.(*Session).Begin"), s)
	defer func() {
		onDone(err, tx)
	}()

	tx, err = begin(ctx, s.grpcClient, s, txSettings)
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
	onDone := trace.QueryOnSessionExecute(s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.(*Session).Execute"), s, q)
	defer func() {
		onDone(err)
	}()

	tx, r, err := execute(ctx, s, s.grpcClient, q, options.ExecuteSettings(opts...))
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}

	return tx, r, nil
}
