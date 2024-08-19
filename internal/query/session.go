package query

import (
	"context"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ query.Session = (*Session)(nil)

type Session struct {
	cfg                *config.Config
	id                 string
	queryServiceClient Ydb_Query_V1.QueryServiceClient
	nodeID             uint32
	statusCode         statusCode
	closeOnce          func(ctx context.Context) error
	checks             []func(s *Session) bool
}

func (s *Session) QueryResultSet(
	ctx context.Context, q string, opts ...options.Execute,
) (rs query.ResultSet, finalErr error) {
	onDone := trace.QueryOnSessionQueryResultSet(s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).QueryResultSet"), s, q)
	defer func() {
		onDone(finalErr)
	}()

	_, r, err := execute(ctx, s.id, s.queryServiceClient, q, options.ExecuteSettings(opts...), withTrace(s.cfg.Trace()))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	rs, err = readResultSet(ctx, r)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return rs, nil
}

func (s *Session) queryRow(
	ctx context.Context, q string, settings executeSettings, resultOpts ...resultOption,
) (row query.Row, finalErr error) {
	_, r, err := execute(ctx, s.id, s.queryServiceClient, q, settings, resultOpts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	row, err = readRow(ctx, r)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}

func (s *Session) QueryRow(ctx context.Context, q string, opts ...options.Execute) (_ query.Row, finalErr error) {
	onDone := trace.QueryOnSessionQueryRow(s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).QueryRow"), s, q)
	defer func() {
		onDone(finalErr)
	}()

	row, err := s.queryRow(ctx, q, options.ExecuteSettings(opts...), withTrace(s.cfg.Trace()))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}

func createSession(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, cfg *config.Config) (
	s *Session, finalErr error,
) {
	s = &Session{
		cfg:                cfg,
		queryServiceClient: client,
		statusCode:         statusUnknown,
		checks: []func(s *Session) bool{
			func(s *Session) bool {
				switch s.status() {
				case statusClosed, statusClosing:
					return false
				default:
					return true
				}
			},
		},
	}

	onDone := trace.QueryOnSessionCreate(s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.createSession"),
	)
	defer func() {
		if s == nil && finalErr == nil {
			panic("abnormal result: both nil")
		}
		if s != nil && finalErr != nil {
			panic("abnormal result: both not nil")
		}

		if finalErr != nil {
			onDone(nil, finalErr)
		} else if s != nil {
			onDone(s, nil)
		}
	}()

	response, err := client.CreateSession(ctx, &Ydb_Query.CreateSessionRequest{})
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	s.id = response.GetSessionId()
	s.nodeID = uint32(response.GetNodeId())

	err = s.attach(ctx)
	if err != nil {
		_ = deleteSession(ctx, client, response.GetSessionId())

		return nil, xerrors.WithStackTrace(err)
	}

	s.setStatus(statusIdle)

	return s, nil
}

func (s *Session) attach(ctx context.Context) (finalErr error) {
	onDone := trace.QueryOnSessionAttach(s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).attach"), s)
	defer func() {
		onDone(finalErr)
	}()

	attachCtx, cancelAttach := xcontext.WithCancel(xcontext.ValueOnly(ctx))
	defer func() {
		if finalErr != nil {
			cancelAttach()
		}
	}()

	attach, err := s.queryServiceClient.AttachSession(attachCtx, &Ydb_Query.AttachSessionRequest{
		SessionId: s.id,
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	_, err = attach.Recv()
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	s.closeOnce = xsync.OnceFunc(s.closeAndDeleteSession(cancelAttach))

	go func() {
		defer func() {
			_ = s.closeOnce(xcontext.ValueOnly(ctx))
		}()

		for func() bool {
			_, recvErr := attach.Recv()

			return recvErr == nil
		}() {
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

		if err = deleteSession(ctx, s.queryServiceClient, s.id); err != nil {
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
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Close"), s)
	defer func() {
		onDone(err)
	}()

	if s.closeOnce != nil {
		return s.closeOnce(ctx)
	}

	return nil
}

func (s *Session) Begin(
	ctx context.Context,
	txSettings query.TransactionSettings,
) (
	_ query.Transaction, err error,
) {
	onDone := trace.QueryOnSessionBegin(s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Begin"), s)
	defer func() {
		onDone(err, tx.ID("lazy"))
	}()

	return &Transaction{
		s:          s,
		txSettings: txSettings,
	}, nil
}

func (s *Session) ID() string {
	return s.id
}

func (s *Session) NodeID() uint32 {
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

func (s *Session) Exec(
	ctx context.Context, q string, opts ...options.Execute,
) (finalErr error) {
	onDone := trace.QueryOnSessionExec(s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Exec"), s, q)
	defer func() {
		onDone(finalErr)
	}()

	_, r, err := execute(ctx, s.id, s.queryServiceClient, q, options.ExecuteSettings(opts...), withTrace(s.cfg.Trace()))
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	err = readAll(ctx, r)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (s *Session) Query(
	ctx context.Context, q string, opts ...options.Execute,
) (_ query.Result, finalErr error) {
	onDone := trace.QueryOnSessionQuery(s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Query"), s, q)
	defer func() {
		onDone(finalErr)
	}()

	_, r, err := execute(ctx, s.id, s.queryServiceClient, q, options.ExecuteSettings(opts...), withTrace(s.cfg.Trace()))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}
