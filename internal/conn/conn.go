package conn

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	// errOperationNotReady specified error when operation is not ready
	errOperationNotReady = xerrors.Wrap(fmt.Errorf("operation is not ready yet"))

	// errClosedConnection specified error when connection are closed early
	errClosedConnection = xerrors.Wrap(fmt.Errorf("connection closed early"))

	// errUnavailableConnection specified error when connection are closed early
	errUnavailableConnection = xerrors.Wrap(fmt.Errorf("connection unavailable"))
)

type Conn interface {
	grpc.ClientConnInterface
	closer.Closer

	Endpoint() endpoint.Endpoint

	LastUsage() time.Time
}

type conn struct {
	*grpc.ClientConn
	config            Config // ro access
	done              chan struct{}
	endpoint          endpoint.Endpoint // ro access
	childStreams      *xcontext.CancelsGuard
	lastUsage         xsync.LastUsage
	onTransportErrors []func(ctx context.Context, cc Conn, cause error)
}

func (c *conn) Address() string {
	return c.endpoint.Address()
}

func (c *conn) LastUsage() time.Time {
	return c.lastUsage.Get()
}

func (c *conn) NodeID() uint32 {
	if c != nil {
		return c.endpoint.NodeID()
	}

	return 0
}

func (c *conn) Endpoint() endpoint.Endpoint {
	if c != nil {
		return c.endpoint
	}

	return nil
}

func (c *conn) onTransportError(ctx context.Context, cause error) {
	for _, onTransportError := range c.onTransportErrors {
		onTransportError(ctx, c, cause)
	}
}

func (c *conn) Close(ctx context.Context) (err error) {
	onDone := trace.DriverOnConnClose(
		c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*conn).Close"),
		c.Endpoint(),
	)
	defer func() {
		onDone(err)
	}()

	c.childStreams.Cancel()

	err = c.ClientConn.Close()
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

var onTransportErrorStub = func(ctx context.Context, err error) {}

//nolint:funlen
func invoke(
	ctx context.Context,
	method string,
	req, reply any,
	cc grpc.ClientConnInterface,
	onTransportError func(context.Context, error),
	address string,
	opts ...grpc.CallOption,
) (
	opID string,
	issues []trace.Issue,
	_ error,
) {
	useWrapping := UseWrapping(ctx)

	ctx, traceID, err := meta.TraceID(ctx)
	if err != nil {
		return opID, issues, xerrors.WithStackTrace(err)
	}

	ctx, sentMark := markContext(meta.WithTraceID(ctx, traceID))

	if onTransportError == nil {
		onTransportError = onTransportErrorStub
	}

	err = cc.Invoke(ctx, method, req, reply, opts...)
	if err != nil {
		if xerrors.IsContextError(err) {
			return opID, issues, xerrors.WithStackTrace(err)
		}

		defer func() {
			onTransportError(ctx, err)
		}()

		if useWrapping {
			if sentMark.canRetry() {
				return opID, issues, xerrors.Retryable(xerrors.Transport(err,
					xerrors.WithAddress(address),
					xerrors.WithTraceID(traceID),
				), xerrors.WithName("Invoke"))
			}

			return opID, issues, xerrors.WithStackTrace(xerrors.Transport(err,
				xerrors.WithAddress(address),
				xerrors.WithTraceID(traceID),
			))
		}

		return opID, issues, err
	}

	switch t := reply.(type) {
	case operation.Response:
		opID = t.GetOperation().GetId()
		for _, issue := range t.GetOperation().GetIssues() {
			issues = append(issues, issue)
		}
		if useWrapping {
			switch {
			case !t.GetOperation().GetReady():
				return opID, issues, xerrors.WithStackTrace(errOperationNotReady)

			case t.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
				return opID, issues, xerrors.WithStackTrace(
					xerrors.Operation(
						xerrors.FromOperation(t.GetOperation()),
						xerrors.WithAddress(address),
						xerrors.WithTraceID(traceID),
					),
				)
			}
		}
	case operation.Status:
		for _, issue := range t.GetIssues() {
			issues = append(issues, issue)
		}
		if useWrapping {
			if t.GetStatus() != Ydb.StatusIds_SUCCESS {
				return opID, issues, xerrors.WithStackTrace(
					xerrors.Operation(
						xerrors.FromOperation(t),
						xerrors.WithAddress(address),
						xerrors.WithTraceID(traceID),
					),
				)
			}
		}
	}

	return opID, issues, nil
}

func (c *conn) Invoke(
	ctx context.Context,
	method string,
	req interface{},
	res interface{},
	opts ...grpc.CallOption,
) (err error) {
	var (
		opID   string
		issues []trace.Issue
		onDone = trace.DriverOnConnInvoke(
			c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*conn).Invoke"),
			c.endpoint, trace.Method(method),
		)
		md = metadata.MD{}
	)
	defer func() {
		meta.CallTrailerCallback(ctx, md)
		onDone(err, issues, opID, c.ClientConn.GetState(), md)
	}()

	stop := c.lastUsage.Start()
	defer stop()

	opID, issues, err = invoke(
		ctx,
		method,
		req,
		res,
		c.ClientConn,
		c.onTransportError,
		c.Address(),
		append(opts, grpc.Trailer(&md))...,
	)

	return err
}

//nolint:funlen
func (c *conn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (_ grpc.ClientStream, finalErr error) {
	var (
		onDone = trace.DriverOnConnNewStream(
			c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*conn).NewStream"),
			c.endpoint.Copy(), trace.Method(method),
		)
		useWrapping = UseWrapping(ctx)
	)

	defer func() {
		onDone(finalErr, c.GetState())
	}()

	stop := c.lastUsage.Start()
	defer stop()

	ctx, traceID, err := meta.TraceID(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	ctx, sentMark := markContext(meta.WithTraceID(ctx, traceID))

	ctx, cancel := c.childStreams.WithCancel(ctx)
	defer func() {
		if finalErr != nil {
			cancel()
		}
	}()

	s := &grpcClientStream{
		parentConn:   c,
		streamCtx:    ctx,
		streamCancel: cancel,
		wrapping:     useWrapping,
		traceID:      traceID,
		sentMark:     sentMark,
	}

	s.stream, err = c.ClientConn.NewStream(ctx, desc, method, append(opts, grpc.OnFinish(s.finish))...)
	if err != nil {
		if xerrors.IsContextError(err) {
			return nil, xerrors.WithStackTrace(err)
		}

		defer func() {
			c.onTransportError(ctx, err)
		}()

		if !useWrapping {
			return nil, err
		}

		if sentMark.canRetry() {
			return nil, xerrors.WithStackTrace(xerrors.Retryable(
				xerrors.Transport(err,
					xerrors.WithAddress(c.Address()),
					xerrors.WithNodeID(c.NodeID()),
					xerrors.WithTraceID(traceID),
				),
				xerrors.WithName("NewStream")),
			)
		}

		return nil, xerrors.WithStackTrace(xerrors.Transport(err,
			xerrors.WithAddress(c.Address()),
			xerrors.WithNodeID(c.NodeID()),
			xerrors.WithTraceID(traceID),
		))
	}

	return s, nil
}

type option func(c *conn)

func WithOnTransportError(onTransportError func(ctx context.Context, cc Conn, cause error)) option {
	return func(c *conn) {
		if onTransportError != nil {
			c.onTransportErrors = append(c.onTransportErrors, onTransportError)
		}
	}
}

func New(e endpoint.Endpoint, config Config, opts ...option) (_ *conn, err error) {
	c := &conn{
		endpoint:     e,
		config:       config,
		done:         make(chan struct{}),
		lastUsage:    xsync.NewLastUsage(),
		childStreams: xcontext.NewCancelsGuard(),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}

	// prepend "ydb" scheme for grpc dns-resolver to find the proper scheme
	// three slashes in "ydb:///" is ok. It needs for good parse scheme in grpc resolver.
	address := "ydb:///" + c.endpoint.Address()

	c.ClientConn, err = grpc.NewClient(address, append( //nolint:staticcheck,nolintlint
		[]grpc.DialOption{
			grpc.WithStatsHandler(statsHandler{}),
			//grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			//	err := invoker(ctx, method, req, reply, cc, opts...)
			//	if err != nil {
			//		return xerrors.WithStackTrace(xerrors.Transport(err,
			//			xerrors.WithNodeID(c.NodeID()),
			//			xerrors.WithAddress(c.endpoint.Address()),
			//		))
			//	}
			//}),
			//grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			//
			//}),
			//grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			//
			//}),
			//grpc_middleware.ChainStreamClient(),
		}, c.config.GrpcDialOptions()...,
	)...)

	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return c, nil
}

var _ stats.Handler = statsHandler{}

type statsHandler struct{}

func (statsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (statsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	switch rpcStats.(type) {
	case *stats.Begin, *stats.End:
	default:
		getContextMark(ctx).markDirty()
	}
}

func (statsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (statsHandler) HandleConn(context.Context, stats.ConnStats) {}

type ctxHandleRPCKey struct{}

var rpcKey = ctxHandleRPCKey{}

func markContext(ctx context.Context) (context.Context, *modificationMark) {
	mark := &modificationMark{}

	return context.WithValue(ctx, rpcKey, mark), mark
}

func getContextMark(ctx context.Context) *modificationMark {
	v := ctx.Value(rpcKey)
	if v == nil {
		return &modificationMark{}
	}

	val, ok := v.(*modificationMark)
	if !ok {
		panic(fmt.Sprintf("unsupported type conversion from %T to *modificationMark", val))
	}

	return val
}

type modificationMark struct {
	dirty atomic.Bool
}

func (m *modificationMark) canRetry() bool {
	return !m.dirty.Load()
}

func (m *modificationMark) markDirty() {
	m.dirty.Store(true)
}
