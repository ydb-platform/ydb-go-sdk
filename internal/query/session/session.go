package session

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	balancerContext "github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	Core interface {
		query.SessionInfo
		closer.Closer
		pool.Item

		SetStatus(code Status)
	}
	core struct {
		cc     grpc.ClientConnInterface
		Client Ydb_Query_V1.QueryServiceClient
		Trace  *trace.Query

		deleteTimeout  time.Duration
		id             string
		nodeID         uint32
		status         atomic.Uint32
		onChangeStatus []func(status Status)
		closeOnce      func(ctx context.Context) error
		checks         []func(s *core) bool
	}
)

func (c *core) ID() string {
	return c.id
}

func (c *core) NodeID() uint32 {
	return c.nodeID
}

func (c *core) statusCode() Status {
	return Status(c.status.Load())
}

func (c *core) SetStatus(status Status) {
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

func (c *core) Status() string {
	return c.statusCode().String()
}

type Option func(*core)

func WithConn(cc grpc.ClientConnInterface) Option {
	return func(c *core) {
		c.cc = cc
	}
}

func OnChangeStatus(onChangeStatus func(status Status)) Option {
	return func(c *core) {
		c.onChangeStatus = append(c.onChangeStatus, onChangeStatus)
	}
}

func WithDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(c *core) {
		c.deleteTimeout = deleteTimeout
	}
}

func WithTrace(t *trace.Query) Option {
	return func(c *core) {
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
) (_ *core, finalErr error) {
	core := &core{
		Client: client,
		Trace:  &trace.Query{},
		checks: []func(s *core) bool{
			func(s *core) bool {
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
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/session.Open"),
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

func (c *core) attach(ctx context.Context) (finalErr error) {
	onDone := trace.QueryOnSessionAttach(c.Trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/session.(*core).attach"),
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

func (c *core) closeAndDelete(cancelAttach context.CancelFunc) func(ctx context.Context) (err error) {
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

func (c *core) deleteSession(ctx context.Context) (finalErr error) {
	onDone := trace.QueryOnSessionDelete(c.Trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/session.(*core).deleteSession"),
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

func (c *core) IsAlive() bool {
	for _, check := range c.checks {
		if !check(c) {
			return false
		}
	}

	return true
}

func (c *core) Close(ctx context.Context) (err error) {
	if c.closeOnce != nil {
		return c.closeOnce(ctx)
	}

	return nil
}
