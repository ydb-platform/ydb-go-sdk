package query

import (
	"context"
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

		Done() <-chan struct{}
		SetStatus(code Status)
	}
	sessionCore struct {
		cc     grpc.ClientConnInterface
		Client Ydb_Query_V1.QueryServiceClient
		Trace  *trace.Query
		done   chan struct{}

		deleteTimeout  time.Duration
		id             string
		nodeID         uint32
		status         atomic.Uint32
		onChangeStatus []func(status Status)
		closeOnce      func(ctx context.Context) error
	}
)

func (core *sessionCore) Done() <-chan struct{} {
	return core.done
}

func (core *sessionCore) ID() string {
	return core.id
}

func (core *sessionCore) NodeID() uint32 {
	return core.nodeID
}

func (core *sessionCore) statusCode() Status {
	return Status(core.status.Load())
}

func (core *sessionCore) SetStatus(status Status) {
	switch Status(core.status.Load()) {
	case StatusClosed, StatusError:
		// nop
	default:
		if old := core.status.Swap(uint32(status)); old != uint32(status) {
			for _, onChangeStatus := range core.onChangeStatus {
				onChangeStatus(Status(old))
			}
		}
	}
}

func (core *sessionCore) Status() string {
	select {
	case <-core.done:
		return StatusClosed.String()
	default:
		return core.statusCode().String()
	}
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

func Open(
	ctx context.Context, client Ydb_Query_V1.QueryServiceClient, opts ...Option,
) (_ *sessionCore, finalErr error) {
	core := &sessionCore{
		Client: client,
		Trace:  &trace.Query{},
		done:   make(chan struct{}),
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

func (core *sessionCore) attach(ctx context.Context) (finalErr error) {
	onDone := trace.QueryOnSessionAttach(core.Trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*sessionCore).attach"),
		core,
	)
	defer func() {
		onDone(finalErr)
	}()

	attachCtx, cancelAttach := xcontext.WithDone(xcontext.ValueOnly(ctx), core.done)
	defer func() {
		if finalErr != nil {
			cancelAttach()
		}
	}()

	attach, err := core.Client.AttachSession(attachCtx, &Ydb_Query.AttachSessionRequest{
		SessionId: core.id,
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	_, err = attach.Recv()
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	core.closeOnce = xsync.OnceFunc(func(ctx context.Context) error {
		defer cancelAttach()

		core.SetStatus(StatusClosing)
		defer core.SetStatus(StatusClosed)

		if err = core.deleteSession(ctx); err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	})

	go func() {
		defer func() {
			select {
			case <-core.done:
				return
			default:
				close(core.done)
			}
		}()

		for core.IsAlive() {
			if _, recvErr := attach.Recv(); recvErr != nil {
				return
			}
		}
	}()

	return nil
}

func (core *sessionCore) deleteSession(ctx context.Context) (finalErr error) {
	onDone := trace.QueryOnSessionDelete(core.Trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*sessionCore).deleteSession"),
		core,
	)
	defer func() {
		onDone(finalErr)
	}()

	if d := core.deleteTimeout; d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = xcontext.WithTimeout(ctx, d)
		defer cancel()
	}

	_, err := core.Client.DeleteSession(ctx,
		&Ydb_Query.DeleteSessionRequest{
			SessionId: core.id,
		},
	)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (core *sessionCore) IsAlive() bool {
	select {
	case <-core.done:
		return false
	default:
		return IsAlive(Status(core.status.Load()))
	}
}

func (core *sessionCore) Close(ctx context.Context) (err error) {
	select {
	case <-core.done:
		return nil
	default:
		close(core.done)
	}

	return core.closeOnce(ctx)
}

func StatusFromErr(err error) Status {
	if err == nil {
		panic("err must be not nil")
	}

	switch {
	case xerrors.IsTransportError(err):
		return StatusError
	case xerrors.IsOperationError(err, Ydb.StatusIds_SESSION_BUSY, Ydb.StatusIds_BAD_SESSION):
		return StatusError
	case xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION):
		return StatusClosed
	default:
		return StatusUnknown
	}
}

func applyStatusByError(s interface{ SetStatus(status Status) }, err error) {
	if status := StatusFromErr(err); status != StatusUnknown {
		s.SetStatus(status)
	}
}
