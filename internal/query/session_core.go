package query

import (
	"context"
	"os"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	balancerContext "github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
var markGoroutineWithLabelNodeIDForAttachStream = os.Getenv("YDB_QUERY_SESSION_ATTACH_STREAM_GOROUTINE_LABEL") == "1"

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
		closed atomic.Bool

		deleteTimeout  time.Duration
		id             string
		nodeID         uint32
		status         atomic.Uint32
		onChangeStatus []func(status Status)
		onNodeShutdown func(cause error)
		cancelAttach   context.CancelFunc

		registerCloseCancel func(context.CancelFunc) func()
	}
)

func (core *sessionCore) ID() string {
	if core == nil {
		return ""
	}

	return core.id
}

func (core *sessionCore) NodeID() uint32 {
	if core == nil {
		return 0
	}

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
				onChangeStatus(status)
			}
		}
	}
}

func (core *sessionCore) Status() string {
	if core == nil {
		return StatusUnknown.String()
	}

	if core.closed.Load() {
		return StatusClosed.String()
	}

	return core.statusCode().String()
}

func (core *sessionCore) checkCloseHint(md metadata.MD) {
	for header, values := range md {
		if header != meta.HeaderServerHints {
			continue
		}
		for _, hint := range values {
			if hint == meta.HintSessionClose {
				core.SetStatus(StatusClosing)
			}
		}
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
		c.Trace = gtrace.Compose(c.Trace, t)
	}
}

func WithRegisterCloseCancel(register func(context.CancelFunc) func()) Option {
	return func(c *sessionCore) {
		c.registerCloseCancel = register
	}
}

func Open(
	ctx context.Context, client Ydb_Query_V1.QueryServiceClient, opts ...Option,
) (_ *sessionCore, finalErr error) {
	core := &sessionCore{
		Client: client,
		Trace:  &trace.Query{},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(core)
		}
	}

	onDone := gtrace.QueryOnSessionCreate(core.Trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.Open"),
	)
	defer func() {
		if finalErr == nil {
			onDone(core, nil)
		} else {
			onDone(nil, finalErr)
		}
	}()

	response, err := client.CreateSession(
		balancer.BanOnOperationError(ctx, Ydb.StatusIds_OVERLOADED),
		&Ydb_Query.CreateSessionRequest{},
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if core.cc != nil {
		core.Client = Ydb_Query_V1.NewQueryServiceClient(
			conn.WithContextModifier(core.cc, func(ctx context.Context) context.Context {
				return meta.WithTrailerCallback(balancerContext.WithNodeID(ctx, core.NodeID()), core.checkCloseHint)
			}),
		)
	}

	core.id = response.GetSessionId()
	core.nodeID = uint32(response.GetNodeId())

	if err = core.attach(ctx); err != nil {
		_ = core.deleteSession(ctx)

		return nil, xerrors.WithStackTrace(err)
	}

	core.SetStatus(StatusIdle)

	return core, nil
}

func (core *sessionCore) attach(ctx context.Context) (finalErr error) {
	onDone := gtrace.QueryOnSessionAttach(core.Trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*sessionCore).attach"),
		core,
	)
	defer func() {
		onDone(finalErr)
	}()

	attachCtx, cancelAttach := context.WithCancel(xcontext.ValueOnly(ctx))
	defer func() {
		if finalErr != nil {
			cancelAttach()
		}
	}()

	attachStream, err := core.Client.AttachSession(attachCtx, &Ydb_Query.AttachSessionRequest{
		SessionId: core.id,
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	msg, err := attachStream.Recv()
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	switch {
	case msg.GetSessionShutdown() != nil:
		return xerrors.WithStackTrace(errSessionShutdownHint)
	case msg.GetNodeShutdown() != nil:
		conn.Ban(attachStream.Context(), errNodeShutdownHint)

		return xerrors.WithStackTrace(errNodeShutdownHint)
	}

	core.cancelAttach = cancelAttach
	core.onNodeShutdown = func(cause error) {
		conn.Ban(attachStream.Context(), cause)
	}

	if markGoroutineWithLabelNodeIDForAttachStream {
		pprof.Do(ctx, pprof.Labels(
			"node_id", strconv.Itoa(int(core.NodeID())),
		), func(context.Context) {
			go core.listenAttachStream(attachStream)
		})
	} else {
		go core.listenAttachStream(attachStream)
	}

	return nil
}

func (core *sessionCore) listenAttachStream(attachStream Ydb_Query_V1.QueryService_AttachSessionClient) {
	for core.IsAlive() {
		msg, recvErr := attachStream.Recv()
		if recvErr != nil {
			core.releaseSession()

			return
		}

		if msg.GetSessionShutdown() != nil {
			core.releaseSession()

			return
		}
		if msg.GetNodeShutdown() != nil {
			core.onNodeShutdown(errNodeShutdownHint)
			core.releaseSession()

			return
		}
	}
}

type deleteSessionClient interface {
	DeleteSession(
		ctx context.Context, in *Ydb_Query.DeleteSessionRequest, opts ...grpc.CallOption,
	) (*Ydb_Query.DeleteSessionResponse, error)
}

func deleteSession(ctx context.Context,
	client deleteSessionClient, sessionID string, deleteTimeout time.Duration,
) (finalErr error) {
	if d := deleteTimeout; d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = xcontext.WithTimeout(ctx, d)
		defer cancel()
	}

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

func (core *sessionCore) deleteSession(ctx context.Context) (finalErr error) {
	onDone := gtrace.QueryOnSessionDelete(core.Trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*sessionCore).deleteSession"),
		core,
	)
	defer func() {
		onDone(finalErr)
	}()

	if err := ctx.Err(); err != nil {
		if core.registerCloseCancel != nil {
			deleteCtx, cancelDelete := xcontext.WithCancel(xcontext.ValueOnly(ctx))
			unregister := core.registerCloseCancel(cancelDelete)
			go func() {
				defer func() {
					cancelDelete()
					unregister()
				}()

				_ = deleteSession(deleteCtx, core.Client, core.id, core.deleteTimeout)
			}()
		}

		return nil
	}

	if err := deleteSession(ctx, core.Client, core.id, core.deleteTimeout); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (core *sessionCore) IsAlive() bool {
	if core.closed.Load() {
		return false
	}

	return IsAlive(Status(core.status.Load()))
}

func (core *sessionCore) releaseSession() {
	if core.cancelAttach != nil {
		core.cancelAttach()
	}

	core.SetStatus(StatusClosed)
}

func (core *sessionCore) Close(ctx context.Context) (err error) {
	if !core.closed.CompareAndSwap(false, true) {
		return nil
	}

	core.SetStatus(StatusClosing)

	err = core.deleteSession(ctx)
	if err != nil {
		err = xerrors.WithStackTrace(err)
	}

	core.releaseSession()

	return err
}

func StatusFromErr(err error) Status {
	if err == nil {
		panic("err must be not nil")
	}

	switch {
	case xerrors.IsTransportError(err):
		return StatusError
	case xerrors.IsOperationError(err, Ydb.StatusIds_SESSION_BUSY):
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
