package conn

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	// errOperationNotReady specified error when operation is not ready
	errOperationNotReady = xerrors.Wrap(fmt.Errorf("operation is not ready yet"))

	errConnectionUnderShutdown = xerrors.Retryable(xerrors.Wrap(fmt.Errorf("connection under shutdown")))
)

type Conn interface {
	grpc.ClientConnInterface

	Endpoint() endpoint.Endpoint
	State() state.State
	InFlight() int64
	Unban(ctx context.Context)
	Ban(ctx context.Context)
}

type (
	connConfig interface {
		Trace() *trace.Driver
		DialTimeout() time.Duration
		GrpcChannelShutdownTimeout() time.Duration
		GrpcDialOptions() []grpc.DialOption
	}
	grpcClientConnInterface interface {
		grpc.ClientConnInterface
		io.Closer

		GetState() connectivity.State
	}
	conn struct {
		mtx                     sync.RWMutex
		config                  connConfig // ro access
		grpcConn                grpcClientConnInterface
		endpoint                endpoint.Endpoint // ro access
		cancelDial              atomic.Pointer[context.CancelFunc]
		state                   atomic.Uint32
		inFlight                atomic.Int64
		lastClusterAnnouncement atomic.Int64
		usage                   *usageTracker
		onClose                 []func(*conn)

		streamsMu    sync.Mutex
		childStreams map[*context.CancelFunc]struct{}
		streamsDone  chan struct{}
	}
)

func (c *conn) Address() string {
	return c.endpoint.Address()
}

func (c *conn) NodeID() uint32 {
	if c != nil {
		return c.endpoint.NodeID()
	}

	return 0
}

func (c *conn) Endpoint() endpoint.Endpoint {
	if c != nil {
		return c.endpoint.Copy(
			endpoint.WithLastUpdated(time.Unix(c.lastClusterAnnouncement.Load(), 0)),
		)
	}

	return nil
}

func SetState(ctx context.Context, c Conn, s state.State) state.State {
	if cc, ok := c.(*conn); ok {
		return cc.setState(ctx, s)
	}

	return s
}

func (c *conn) setState(ctx context.Context, s state.State) state.State {
	previous, changed := c.transitionState(s)
	if !changed {
		return previous
	}
	c.traceStateChange(ctx, previous, s)

	return s
}

func (c *conn) transitionState(next state.State) (state.State, bool) {
	for {
		previous := c.State()
		if previous == next || previous.IsClosing() && next != state.Destroyed {
			return previous, false
		}
		if c.state.CompareAndSwap(uint32(previous), uint32(next)) {
			return previous, true
		}
	}
}

func (c *conn) traceStateChange(ctx context.Context, previous, next state.State) {
	gtrace.DriverOnConnStateChange(
		c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).traceStateChange"),
		c.Endpoint(), previous,
	)(next)
}

func (c *conn) unban(ctx context.Context) state.State {
	var newState state.State
	c.mtx.RLock()
	cc := c.grpcConn
	c.mtx.RUnlock()
	if isAvailable(cc) {
		newState = state.Online
	} else {
		newState = state.Offline
	}

	return c.setState(ctx, newState)
}

func (c *conn) Unban(ctx context.Context) {
	gtrace.DriverOnConnAllow(
		c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).Unban"),
		c.Endpoint(), c.State(),
	)(c.unban(ctx))
}

func (c *conn) Ban(ctx context.Context) {
	c.setState(ctx, state.Banned)
}

func (c *conn) State() (s state.State) {
	return state.State(c.state.Load())
}

func (c *conn) InFlight() int64 {
	return c.inFlight.Load()
}

func (c *conn) realConn(ctx context.Context) (cc grpcClientConnInterface, err error) {
	if c.State().IsClosing() {
		return nil, xerrors.WithStackTrace(errConnectionUnderShutdown)
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.State().IsClosing() {
		return nil, xerrors.WithStackTrace(errConnectionUnderShutdown)
	}
	if c.grpcConn != nil {
		return c.grpcConn, nil
	}

	return c.dial(ctx)
}

// c.mtx must be locked
func (c *conn) dial(ctx context.Context) (cc grpcClientConnInterface, err error) {
	onDone := gtrace.DriverOnConnDial(
		c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).dial"),
		c.Endpoint(),
	)
	defer func() {
		onDone(err)
	}()

	ctx, cancelDial := context.WithCancel(ctx)
	c.cancelDial.Store(&cancelDial)
	defer func() {
		c.cancelDial.Store(nil)
		cancelDial()
	}()
	if c.State().IsClosing() {
		return nil, xerrors.WithStackTrace(errConnectionUnderShutdown)
	}

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
	dialOpts = append(dialOpts, grpc.WithChainStreamInterceptor(FallbackCodecStreamInterceptor))

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

func (c *conn) Close(ctx context.Context) (err error) {
	previous, changed := c.transitionState(state.UnderShutdown)
	if !changed {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.config.GrpcChannelShutdownTimeout())
	defer cancel()

	if cancelDial := c.cancelDial.Load(); cancelDial != nil {
		(*cancelDial)()
	}

	if c.hasChildStreams() {
		select {
		case <-c.streamsDone:
		case <-ctx.Done():
		}
	}
	for cancelStream := range c.takeChildStreams() {
		(*cancelStream)()
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.traceStateChange(ctx, previous, state.UnderShutdown)
	onDone := gtrace.DriverOnConnClose(
		c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).Close"),
		c.Endpoint(),
	)
	defer func() {
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

func (c *conn) addChildStream(ctx context.Context) (context.Context, context.CancelFunc) {
	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()
	if c.State().IsClosing() {
		return nil, nil
	}

	streamCtx, cancel := xcontext.WithCancel(ctx)
	c.childStreams[&cancel] = struct{}{}

	return streamCtx, func() {
		cancel()
		c.streamsMu.Lock()
		defer c.streamsMu.Unlock()
		if _, ok := c.childStreams[&cancel]; ok {
			delete(c.childStreams, &cancel)
			if len(c.childStreams) == 0 && c.State().IsClosing() {
				close(c.streamsDone)
			}
		}
	}
}

func (c *conn) hasChildStreams() bool {
	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()

	return len(c.childStreams) != 0
}

func (c *conn) takeChildStreams() map[*context.CancelFunc]struct{} {
	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()
	streams := c.childStreams
	c.childStreams = nil

	return streams
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
	req any,
	res any,
	opts ...grpc.CallOption,
) (err error) {
	if c.State().IsClosing() {
		return xerrors.WithStackTrace(errConnectionUnderShutdown)
	}

	c.inFlight.Add(1)
	defer c.inFlight.Add(-1)

	stopUsage := c.startUsage()
	defer stopUsage()

	var (
		opID   string
		issues []trace.Issue
		onDone = gtrace.DriverOnConnInvoke(
			c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).Invoke"),
			c.Endpoint(), trace.Method(method),
		)
		cc grpcClientConnInterface
		md = metadata.MD{}
	)
	defer func() {
		meta.CallTrailerCallback(ctx, md)
		onDone(err, issues, opID, c.State(), md)
	}()

	cc, err = c.realConn(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

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
	if c.State().IsClosing() {
		return nil, xerrors.WithStackTrace(errConnectionUnderShutdown)
	}

	c.inFlight.Add(1)
	stopInFlight := sync.OnceFunc(func() { c.inFlight.Add(-1) })
	defer func() {
		if finalErr != nil {
			stopInFlight()
		}
	}()

	stopUsage := c.startUsage()
	defer stopUsage()

	var (
		onDone = gtrace.DriverOnConnNewStream(
			c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).NewStream"),
			c.Endpoint(), trace.Method(method),
		)
		useWrapping = UseWrapping(ctx)
	)

	defer func() {
		onDone(finalErr, c.State())
	}()

	ctx, traceID, err := meta.TraceID(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	ctx, sentMark := markContext(meta.WithTraceID(ctx, traceID))

	grpcCtx, cancel := c.addChildStream(ctx)
	if cancel == nil {
		return nil, xerrors.WithStackTrace(errConnectionUnderShutdown)
	}
	defer func() {
		if finalErr != nil {
			cancel()
		}
	}()

	cc, err := c.realConn(grpcCtx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	s := &grpcClientStream{
		parentConn: c,
		requestCtx: ctx,
		grpcCancel: cancel,
		wrapping:   useWrapping,
		traceID:    traceID,
		sentMark:   sentMark,
	}

	s.stream, err = cc.NewStream(grpcCtx, desc, method, append(opts, grpc.OnFinish(func(err error) {
		stopInFlight()
		s.finish(err)
	}))...)
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

func withUsageTracker(clock clockwork.Clock, enabled bool) option {
	return func(c *conn) {
		if enabled {
			c.usage = newUsageTracker(clock)
		}
	}
}

func newConn(e endpoint.Endpoint, config connConfig, opts ...option) *conn {
	c := &conn{
		endpoint:     e,
		config:       config,
		childStreams: make(map[*context.CancelFunc]struct{}),
		streamsDone:  make(chan struct{}),
	}

	c.state.Store(uint32(state.Created))
	c.lastClusterAnnouncement.Store(time.Now().Unix())

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
