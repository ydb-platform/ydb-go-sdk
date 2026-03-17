package conn

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
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
)

type Conn interface {
	grpc.ClientConnInterface

	Endpoint() endpoint.Endpoint
	LastUsage() time.Time
	GetState() state.State
	SetState(ctx context.Context, state state.State) state.State
	Unban(ctx context.Context) state.State
}

type (
	connConfig interface {
		Trace() *trace.Driver
		DialTimeout() time.Duration
		GrpcDialOptions() []grpc.DialOption
	}
	grpcClientConnInterface interface {
		grpc.ClientConnInterface
		io.Closer

		GetState() connectivity.State
	}
	conn struct {
		mtx          sync.RWMutex
		config       connConfig // ro access
		grpcConn     grpcClientConnInterface
		done         chan struct{}
		endpoint     endpoint.Endpoint // ro access
		closed       bool
		state        atomic.Uint32
		childStreams *xcontext.CancelsGuard
		lastUsage    xsync.LastUsage
		onClose      []func(*conn)
	}
)

func (c *conn) Address() string {
	return c.endpoint.Address()
}

func (c *conn) LastUsage() time.Time {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	return c.lastUsage.Get()
}

func (c *conn) NodeID() uint32 {
	if c != nil {
		return c.endpoint.NodeID()
	}

	return 0
}

func (c *conn) park(ctx context.Context) (err error) {
	onDone := trace.DriverOnConnPark(
		c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).park"),
		c.Endpoint(),
	)
	defer func() {
		onDone(err)
	}()

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.closed {
		return nil
	}

	if c.grpcConn == nil {
		return nil
	}

	err = c.close(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *conn) Endpoint() endpoint.Endpoint {
	if c != nil {
		return c.endpoint
	}

	return nil
}

func (c *conn) SetState(ctx context.Context, s state.State) state.State {
	return c.setState(ctx, s)
}

func (c *conn) setState(ctx context.Context, s state.State) state.State {
	if state := state.State(c.state.Swap(uint32(s))); state != s {
		trace.DriverOnConnStateChange(
			c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).setState"),
			c.endpoint.Copy(), state,
		)(s)
	}

	return s
}

func (c *conn) Unban(ctx context.Context) state.State {
	var newState state.State
	c.mtx.RLock()
	cc := c.grpcConn
	c.mtx.RUnlock()
	if isAvailable(cc) {
		newState = state.Online
	} else {
		newState = state.Offline
	}

	c.setState(ctx, newState)

	return newState
}

func (c *conn) GetState() (s state.State) {
	return state.State(c.state.Load())
}

func (c *conn) realConn(ctx context.Context) (cc grpcClientConnInterface, err error) {
	if c.isClosed() {
		return nil, xerrors.WithStackTrace(errClosedConnection)
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.grpcConn != nil {
		return c.grpcConn, nil
	}

	return c.dial(ctx)
}

// c.mtx must be locked
func (c *conn) dial(ctx context.Context) (cc grpcClientConnInterface, err error) {
	onDone := trace.DriverOnConnDial(
		c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).dial"),
		c.endpoint.Copy(),
	)
	defer func() {
		onDone(err)
	}()

	if dialTimeout := c.config.DialTimeout(); dialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = xcontext.WithTimeout(ctx, dialTimeout)
		defer cancel()
	}

	address := c.endpoint.Address()

	dialOpts := append([]grpc.DialOption{}, c.config.GrpcDialOptions()...)

	dialOpts = append(dialOpts, grpc.WithStatsHandler(statsHandler{}))

	if overrideHost := c.endpoint.OverrideHost(); overrideHost != "" {
		dialOpts = append(dialOpts, grpc.WithAuthority(overrideHost))
	}

	//nolint:staticcheck,nolintlint
	cc, err = grpc.DialContext(ctx, address, dialOpts...)
	if err != nil {
		if xerrors.IsContextError(err) {
			return nil, xerrors.WithStackTrace(err)
		}

		return nil, xerrors.WithStackTrace(
			xerrors.Retryable(
				xerrors.Transport(err),
				xerrors.WithName(
					stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).dial").String(),
				),
			),
		)
	}

	c.grpcConn = cc
	c.setState(ctx, state.Online)

	return c.grpcConn, nil
}

func isAvailable(raw grpcClientConnInterface) bool {
	return raw != nil && raw.GetState() == connectivity.Ready
}

// conn must be locked
func (c *conn) close(ctx context.Context) (err error) {
	if c.grpcConn == nil {
		return nil
	}

	defer func() {
		c.grpcConn = nil
		c.setState(ctx, state.Offline)
	}()

	err = c.grpcConn.Close()
	if err == nil || !UseWrapping(ctx) {
		return err
	}

	return xerrors.WithStackTrace(err)
}

func (c *conn) isClosed() bool {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	return c.closed
}

func (c *conn) Close(ctx context.Context) (err error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.closed {
		return nil
	}

	onDone := trace.DriverOnConnClose(
		c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).Close"),
		c.Endpoint(),
	)
	defer func() {
		c.closed = true

		c.setState(ctx, state.Destroyed)

		for _, onClose := range c.onClose {
			onClose(c)
		}

		onDone(err)
	}()

	err = c.close(ctx)

	if !UseWrapping(ctx) {
		return err
	}

	return xerrors.WithStackTrace(xerrors.Transport(err,
		xerrors.WithAddress(c.Address()),
		xerrors.WithNodeID(c.NodeID()),
	))
}

func replyWrapper(reply any) (opID string, issues []trace.Issue) {
	switch t := reply.(type) {
	case operation.Response:
		opID = t.GetOperation().GetId()
		for _, issue := range t.GetOperation().GetIssues() {
			issues = append(issues, issue)
		}
	case operation.Status:
		for _, issue := range t.GetIssues() {
			issues = append(issues, issue)
		}
	}

	return opID, issues
}

//nolint:funlen
func invoke(
	ctx context.Context,
	method string,
	req, reply any,
	cc grpc.ClientConnInterface,
	address string,
	nodeID uint32,
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

	err = cc.Invoke(ctx, method, req, reply, opts...)
	if err != nil {
		if xerrors.IsContextError(err) {
			return opID, issues, xerrors.WithStackTrace(err)
		}
		if ctxErr := ctx.Err(); ctxErr != nil &&
			(status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded) {
			return opID, issues, xerrors.WithStackTrace(ctxErr)
		}

		if !useWrapping {
			return opID, issues, err
		}

		if sentMark.canRetry() {
			return opID, issues, xerrors.WithStackTrace(xerrors.Retryable(
				xerrors.Transport(err,
					xerrors.WithTraceID(traceID),
				),
				xerrors.WithName("Invoke"),
			))
		}

		return opID, issues, xerrors.WithStackTrace(xerrors.Transport(err,
			xerrors.WithAddress(address),
			xerrors.WithNodeID(nodeID),
			xerrors.WithTraceID(traceID),
		))
	}

	opID, issues = replyWrapper(reply)

	if !useWrapping {
		return opID, issues, nil
	}

	switch t := reply.(type) {
	case operation.Response:
		switch {
		case !t.GetOperation().GetReady():
			return opID, issues, xerrors.WithStackTrace(errOperationNotReady)

		case t.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
			return opID, issues, xerrors.WithStackTrace(
				xerrors.Operation(
					xerrors.FromOperation(t.GetOperation()),
					xerrors.WithAddress(address),
					xerrors.WithNodeID(nodeID),
					xerrors.WithTraceID(traceID),
				),
			)
		}
	case operation.Status:
		if t.GetStatus() != Ydb.StatusIds_SUCCESS {
			return opID, issues, xerrors.WithStackTrace(
				xerrors.Operation(
					xerrors.FromOperation(t),
					xerrors.WithAddress(address),
					xerrors.WithNodeID(nodeID),
					xerrors.WithTraceID(traceID),
				),
			)
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
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).Invoke"),
			c.endpoint, trace.Method(method),
		)
		cc grpcClientConnInterface
		md = metadata.MD{}
	)
	defer func() {
		meta.CallTrailerCallback(ctx, md)
		onDone(err, issues, opID, c.GetState(), md)
	}()

	cc, err = c.realConn(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	stop := c.lastUsage.Start()
	defer stop()

	opID, issues, err = invoke(
		ctx,
		method,
		req,
		res,
		cc,
		c.Address(),
		c.NodeID(),
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
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).NewStream"),
			c.endpoint.Copy(), trace.Method(method),
		)
		useWrapping = UseWrapping(ctx)
	)

	defer func() {
		onDone(finalErr, c.GetState())
	}()

	cc, err := c.realConn(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

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

	s.stream, err = cc.NewStream(ctx, desc, method, append(opts, grpc.OnFinish(s.finish))...)
	if err != nil {
		if xerrors.IsContextError(err) {
			return nil, xerrors.WithStackTrace(err)
		}

		if !useWrapping {
			return nil, err
		}

		if sentMark.canRetry() {
			return nil, xerrors.WithStackTrace(xerrors.Retryable(
				xerrors.Transport(err,
					xerrors.WithTraceID(traceID),
				),
				xerrors.WithName("NewStream"),
			))
		}

		return nil, xerrors.WithStackTrace(xerrors.Transport(err,
			xerrors.WithAddress(c.Address()),
			xerrors.WithTraceID(traceID),
		))
	}

	return s, nil
}

type option func(c *conn)

func withOnClose(onClose func(*conn)) option {
	return func(c *conn) {
		if onClose != nil {
			c.onClose = append(c.onClose, onClose)
		}
	}
}

func newConn(e endpoint.Endpoint, config connConfig, opts ...option) *conn {
	c := &conn{
		endpoint:     e,
		config:       config,
		done:         make(chan struct{}),
		lastUsage:    xsync.NewLastUsage(),
		childStreams: xcontext.NewCancelsGuard(),
		onClose: []func(*conn){
			func(c *conn) {
				c.childStreams.Cancel()
			},
		},
	}
	c.state.Store(uint32(state.Created))
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}

	return c
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
