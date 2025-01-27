package query

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	balancerContext "github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	baseTx "github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ query.Session = (*Session)(nil)

type (
	Core interface {
		query.SessionInfo
		closer.Closer
		pool.Item

		SetStatus(code Status)
	}
	sessionCore struct {
		cc     grpc.ClientConnInterface
		Client Ydb_Query_V1.QueryServiceClient
		Trace  *trace.Query

		deleteTimeout  time.Duration
		id             string
		nodeID         uint32
		status         atomic.Uint32
		onChangeStatus []func(status Status)
		closeOnce      func(ctx context.Context) error
		checks         []func(s *sessionCore) bool
	}
	Session struct {
		Core

		client Ydb_Query_V1.QueryServiceClient
		trace  *trace.Query
		lazyTx bool
	}
)

const (
	statusUnknown = Status(iota)
	StatusIdle
	StatusInUse
	StatusClosing
	StatusClosed
	StatusError
)

func (s Status) String() string {
	switch s {
	case statusUnknown:
		return "Unknown"
	case StatusIdle:
		return "Idle"
	case StatusInUse:
		return "InUse"
	case StatusClosing:
		return "Closing"
	case StatusClosed:
		return "Closed"
	case StatusError:
		return "Error"
	default:
		return fmt.Sprintf("Unknown%d", s)
	}
}

func (c *sessionCore) ID() string {
	return c.id
}

func (c *sessionCore) NodeID() uint32 {
	return c.nodeID
}

func (c *sessionCore) statusCode() Status {
	return Status(c.status.Load())
}

func (c *sessionCore) SetStatus(status Status) {
	switch Status(c.status.Load()) {
	case StatusClosed, StatusError:
		// nop
	default:
		if old := c.status.Swap(uint32(status)); old != uint32(status) {
			for _, onChangeStatus := range c.onChangeStatus {
				onChangeStatus(Status(old))
			}
		}
	}
}

func (c *sessionCore) Status() string {
	return c.statusCode().String()
}

type Option func(*sessionCore)

func WithConn(cc grpc.ClientConnInterface) Option {
	return func(c *sessionCore) {
		c.cc = cc
	}
}

func OnChangeStatus(onChangeStatus func(status Status)) Option {
	return func(c *sessionCore) {
		c.onChangeStatus = append(c.onChangeStatus, onChangeStatus)
	}
}

func WithDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(c *sessionCore) {
		c.deleteTimeout = deleteTimeout
	}
}

func WithTrace(t *trace.Query) Option {
	return func(c *sessionCore) {
		c.Trace = c.Trace.Compose(t)
	}
}

func IsAlive(status Status) bool {
	switch status {
	case StatusClosed, StatusClosing, StatusError:
		return false
	default:
		return true
	}
}

func Open(
	ctx context.Context, client Ydb_Query_V1.QueryServiceClient, opts ...Option,
) (_ *sessionCore, finalErr error) {
	core := &sessionCore{
		Client: client,
		Trace:  &trace.Query{},
		checks: []func(s *sessionCore) bool{
			func(s *sessionCore) bool {
				return IsAlive(Status(s.status.Load()))
			},
		},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(core)
		}
	}

	onDone := trace.QueryOnSessionCreate(core.Trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.Open"),
	)
	defer func() {
		if finalErr == nil {
			onDone(core, nil)
		} else {
			onDone(nil, finalErr)
		}
	}()

	response, err := client.CreateSession(ctx, &Ydb_Query.CreateSessionRequest{})
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if core.cc != nil {
		core.Client = Ydb_Query_V1.NewQueryServiceClient(
			conn.WithContextModifier(core.cc, func(ctx context.Context) context.Context {
				return balancerContext.WithNodeID(ctx, core.NodeID())
			}),
		)
	}

	core.id = response.GetSessionId()
	core.nodeID = uint32(response.GetNodeId())

	err = core.attach(ctx)
	if err != nil {
		_ = core.deleteSession(ctx)

		return nil, xerrors.WithStackTrace(err)
	}

	core.SetStatus(StatusIdle)

	return core, nil
}

func (c *sessionCore) attach(ctx context.Context) (finalErr error) {
	onDone := trace.QueryOnSessionAttach(c.Trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*sessionCore).attach"),
		c,
	)
	defer func() {
		onDone(finalErr)
	}()

	attachCtx, cancelAttach := xcontext.WithCancel(xcontext.ValueOnly(ctx))
	defer func() {
		if finalErr != nil {
			cancelAttach()
		}
	}()

	attach, err := c.Client.AttachSession(attachCtx, &Ydb_Query.AttachSessionRequest{
		SessionId: c.id,
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	_, err = attach.Recv()
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	c.closeOnce = xsync.OnceFunc(c.closeAndDelete(cancelAttach))

	go func() {
		defer func() {
			_ = c.closeOnce(xcontext.ValueOnly(ctx))
		}()

		for c.IsAlive() {
			if _, recvErr := attach.Recv(); recvErr != nil {
				return
			}
		}
	}()

	return nil
}

func (c *sessionCore) closeAndDelete(cancelAttach context.CancelFunc) func(ctx context.Context) (err error) {
	return func(ctx context.Context) (err error) {
		defer cancelAttach()

		c.SetStatus(StatusClosing)
		defer c.SetStatus(StatusClosed)

		var cancel context.CancelFunc
		if d := c.deleteTimeout; d > 0 {
			ctx, cancel = xcontext.WithTimeout(ctx, d)
		} else {
			ctx, cancel = xcontext.WithCancel(ctx)
		}
		defer cancel()

		if err = c.deleteSession(ctx); err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}
}

func (c *sessionCore) deleteSession(ctx context.Context) (finalErr error) {
	onDone := trace.QueryOnSessionDelete(c.Trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*sessionCore).deleteSession"),
		c,
	)
	defer func() {
		onDone(finalErr)
	}()

	_, err := c.Client.DeleteSession(ctx,
		&Ydb_Query.DeleteSessionRequest{
			SessionId: c.id,
		},
	)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *sessionCore) IsAlive() bool {
	for _, check := range c.checks {
		if !check(c) {
			return false
		}
	}

	return true
}

func (c *sessionCore) Close(ctx context.Context) (err error) {
	if c.closeOnce != nil {
		return c.closeOnce(ctx)
	}

	return nil
}

func (s *Session) QueryResultSet(
	ctx context.Context, q string, opts ...options.Execute,
) (rs result.ClosableResultSet, finalErr error) {
	onDone := trace.QueryOnSessionQueryResultSet(s.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).QueryResultSet"), s, q)
	defer func() {
		onDone(finalErr)
	}()

	r, err := execute(ctx, s.ID(), s.client, q, options.ExecuteSettings(opts...), withTrace(s.trace))
	if err != nil {
		s.setStatusFromError(err)

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
	r, err := execute(ctx, s.ID(), s.client, q, settings, resultOpts...)
	if err != nil {
		s.setStatusFromError(err)

		return nil, xerrors.WithStackTrace(err)
	}

	row, err = readRow(ctx, r)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}

func (s *Session) QueryRow(ctx context.Context, q string, opts ...options.Execute) (_ query.Row, finalErr error) {
	onDone := trace.QueryOnSessionQueryRow(s.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).QueryRow"), s, q)
	defer func() {
		onDone(finalErr)
	}()

	row, err := s.queryRow(ctx, q, options.ExecuteSettings(opts...), withTrace(s.trace))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}

func createSession(
	ctx context.Context, client Ydb_Query_V1.QueryServiceClient, opts ...Option,
) (*Session, error) {
	core, err := Open(ctx, client, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return &Session{
		Core:   core,
		trace:  core.Trace,
		client: core.Client,
	}, nil
}

func (s *Session) Begin(
	ctx context.Context,
	txSettings query.TransactionSettings,
) (
	tx query.Transaction, finalErr error,
) {
	onDone := trace.QueryOnSessionBegin(s.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Begin"), s)
	defer func() {
		if finalErr != nil {
			onDone(finalErr, nil)
		} else {
			onDone(nil, tx)
		}
	}()

	if s.lazyTx {
		return &Transaction{
			s:          s,
			txSettings: txSettings,
		}, nil
	}

	txID, err := begin(ctx, s.client, s.ID(), txSettings)
	if err != nil {
		s.setStatusFromError(err)

		return nil, xerrors.WithStackTrace(err)
	}

	return &Transaction{
		LazyID: baseTx.ID(txID),
		s:      s,
	}, nil
}

func (s *Session) Exec(
	ctx context.Context, q string, opts ...options.Execute,
) (finalErr error) {
	onDone := trace.QueryOnSessionExec(s.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Exec"), s, q)
	defer func() {
		onDone(finalErr)
	}()

	r, err := execute(ctx, s.ID(), s.client, q, options.ExecuteSettings(opts...), withTrace(s.trace))
	if err != nil {
		s.setStatusFromError(err)

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
	onDone := trace.QueryOnSessionQuery(s.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Query"), s, q)
	defer func() {
		onDone(finalErr)
	}()

	r, err := execute(ctx, s.ID(), s.client, q, options.ExecuteSettings(opts...), withTrace(s.trace))
	if err != nil {
		s.setStatusFromError(err)

		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func (s *Session) setStatusFromError(err error) {
	switch {
	case xerrors.IsTransportError(err):
		s.SetStatus(StatusError)
	case xerrors.IsOperationError(err, Ydb.StatusIds_SESSION_BUSY, Ydb.StatusIds_BAD_SESSION):
		s.SetStatus(StatusError)
	case xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION):
		s.SetStatus(StatusClosed)
	}
}
