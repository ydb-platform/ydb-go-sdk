package conn

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/response"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xatomic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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

	Endpoint() endpoint.Endpoint

	LastUsage() time.Time

	Ping(ctx context.Context) error
	IsState(states ...State) bool
	GetState() State
	SetState(ctx context.Context, state State) State
	Unban(ctx context.Context) State
}

type conn struct {
	mtx               sync.RWMutex
	config            Config // ro access
	cc                *grpc.ClientConn
	done              chan struct{}
	endpoint          endpoint.Endpoint // ro access
	closed            bool
	state             xatomic.Uint32
	lastUsage         time.Time
	onClose           []func(*conn)
	onTransportErrors []func(ctx context.Context, cc Conn, cause error)
}

func (c *conn) Address() string {
	return c.endpoint.Address()
}

func (c *conn) Ping(ctx context.Context) error {
	cc, err := c.realConn(ctx)
	if err != nil {
		return c.wrapError(err)
	}
	if !isAvailable(cc) {
		return c.wrapError(errUnavailableConnection)
	}
	return nil
}

func (c *conn) LastUsage() time.Time {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.lastUsage
}

func (c *conn) IsState(states ...State) bool {
	state := State(c.state.Load())
	for _, s := range states {
		if s == state {
			return true
		}
	}

	return false
}

func (c *conn) park(ctx context.Context) (err error) {
	onDone := trace.DriverOnConnPark(
		c.config.Trace(), &ctx,
		stack.FunctionID(""),
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

	if c.cc == nil {
		return nil
	}

	err = c.close(ctx)

	if err != nil {
		return c.wrapError(err)
	}

	return nil
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

func (c *conn) SetState(ctx context.Context, s State) State {
	return c.setState(ctx, s)
}

func (c *conn) setState(ctx context.Context, s State) State {
	if state := State(c.state.Swap(uint32(s))); state != s {
		trace.DriverOnConnStateChange(
			c.config.Trace(), &ctx,
			stack.FunctionID(""),
			c.endpoint.Copy(), state,
		)(s)
	}
	return s
}

func (c *conn) Unban(ctx context.Context) State {
	var newState State
	c.mtx.RLock()
	cc := c.cc
	c.mtx.RUnlock()
	if isAvailable(cc) {
		newState = Online
	} else {
		newState = Offline
	}

	c.setState(ctx, newState)
	return newState
}

func (c *conn) GetState() (s State) {
	return State(c.state.Load())
}

func (c *conn) realConn(ctx context.Context) (cc *grpc.ClientConn, err error) {
	if c.isClosed() {
		return nil, c.wrapError(errClosedConnection)
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.cc != nil {
		return c.cc, nil
	}

	if dialTimeout := c.config.DialTimeout(); dialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = xcontext.WithTimeout(ctx, dialTimeout)
		defer cancel()
	}

	onDone := trace.DriverOnConnDial(
		c.config.Trace(), &ctx,
		stack.FunctionID(""),
		c.endpoint.Copy(),
	)
	defer func() {
		onDone(err)
	}()

	// prepend "ydb" scheme for grpc dns-resolver to find the proper scheme
	// three slashes in "ydb:///" is ok. It needs for good parse scheme in grpc resolver.
	address := "ydb:///" + c.endpoint.Address()

	cc, err = grpc.DialContext(ctx, address, append(
		[]grpc.DialOption{
			grpc.WithStatsHandler(statsHandler{}),
		}, c.config.GrpcDialOptions()...,
	)...)
	if err != nil {
		defer func() {
			c.onTransportError(ctx, err)
		}()

		err = xerrors.Transport(err,
			xerrors.WithAddress(address),
		)

		return nil, c.wrapError(
			xerrors.Retryable(err,
				xerrors.WithName("realConn"),
			),
		)
	}

	c.cc = cc
	c.setState(ctx, Online)

	return c.cc, nil
}

func (c *conn) onTransportError(ctx context.Context, cause error) {
	for _, onTransportError := range c.onTransportErrors {
		onTransportError(ctx, c, cause)
	}
}

func (c *conn) touchLastUsage() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.lastUsage = time.Now()
}

func isAvailable(raw *grpc.ClientConn) bool {
	return raw != nil && raw.GetState() == connectivity.Ready
}

// conn must be locked
func (c *conn) close(ctx context.Context) (err error) {
	if c.cc == nil {
		return nil
	}
	err = c.cc.Close()
	c.cc = nil
	c.setState(ctx, Offline)
	return c.wrapError(err)
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
		stack.FunctionID(""),
		c.Endpoint(),
	)
	defer func() {
		onDone(err)
	}()

	c.closed = true

	err = c.close(ctx)

	c.setState(ctx, Destroyed)

	for _, onClose := range c.onClose {
		onClose(c)
	}

	return c.wrapError(err)
}

func (c *conn) Invoke(
	ctx context.Context,
	method string,
	req interface{},
	res interface{},
	opts ...grpc.CallOption,
) (err error) {
	var (
		opID        string
		issues      []trace.Issue
		useWrapping = UseWrapping(ctx)
		onDone      = trace.DriverOnConnInvoke(
			c.config.Trace(), &ctx,
			stack.FunctionID(""),
			c.endpoint, trace.Method(method),
		)
		cc *grpc.ClientConn
		md = metadata.MD{}
	)
	defer func() {
		meta.CallTrailerCallback(ctx, md)
		onDone(err, issues, opID, c.GetState(), md)
	}()

	cc, err = c.realConn(ctx)
	if err != nil {
		return c.wrapError(err)
	}

	c.touchLastUsage()
	defer c.touchLastUsage()

	ctx, traceID, err := meta.TraceID(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	ctx, sentMark := markContext(meta.WithTraceID(ctx, traceID))

	err = cc.Invoke(ctx, method, req, res, append(opts, grpc.Trailer(&md))...)
	if err != nil {
		defer func() {
			c.onTransportError(ctx, err)
		}()

		if useWrapping {
			err = xerrors.Transport(err,
				xerrors.WithAddress(c.Address()),
				xerrors.WithTraceID(traceID),
			)
			if sentMark.canRetry() {
				return c.wrapError(xerrors.Retryable(err, xerrors.WithName("Invoke")))
			}
			return c.wrapError(err)
		}

		return err
	}

	if o, ok := res.(response.Response); ok {
		opID = o.GetOperation().GetId()
		for _, issue := range o.GetOperation().GetIssues() {
			issues = append(issues, issue)
		}
		if useWrapping {
			switch {
			case !o.GetOperation().GetReady():
				return c.wrapError(errOperationNotReady)

			case o.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
				return c.wrapError(
					xerrors.Operation(
						xerrors.FromOperation(o.GetOperation()),
						xerrors.WithAddress(c.Address()),
						xerrors.WithTraceID(traceID),
					),
				)
			}
		}
	}

	return err
}

func (c *conn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (_ grpc.ClientStream, err error) {
	var (
		streamRecv = trace.DriverOnConnNewStream(
			c.config.Trace(), &ctx,
			stack.FunctionID(""),
			c.endpoint.Copy(), trace.Method(method),
		)
		useWrapping = UseWrapping(ctx)
		cc          *grpc.ClientConn
		s           grpc.ClientStream
	)

	defer func() {
		if err != nil {
			streamRecv(err)(err, c.GetState(), metadata.MD{})
		}
	}()

	var cancel context.CancelFunc
	ctx, cancel = xcontext.WithCancel(ctx)

	defer func() {
		if err != nil {
			cancel()
		}
	}()

	cc, err = c.realConn(ctx)
	if err != nil {
		return nil, c.wrapError(err)
	}

	c.touchLastUsage()
	defer c.touchLastUsage()

	ctx, traceID, err := meta.TraceID(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	ctx, sentMark := markContext(meta.WithTraceID(ctx, traceID))

	s, err = cc.NewStream(ctx, desc, method, opts...)
	if err != nil {
		defer func() {
			c.onTransportError(ctx, err)
		}()

		if useWrapping {
			err = xerrors.Transport(err,
				xerrors.WithAddress(c.Address()),
				xerrors.WithTraceID(traceID),
			)
			if sentMark.canRetry() {
				return s, c.wrapError(xerrors.Retryable(err, xerrors.WithName("NewStream")))
			}
			return s, c.wrapError(err)
		}

		return s, err
	}

	return &grpcClientStream{
		ClientStream: s,
		c:            c,
		wrapping:     useWrapping,
		traceID:      traceID,
		sentMark:     sentMark,
		onDone: func(ctx context.Context, md metadata.MD) {
			cancel()
			meta.CallTrailerCallback(ctx, md)
		},
		recv: streamRecv,
	}, nil
}

func (c *conn) wrapError(err error) error {
	if err == nil {
		return nil
	}
	nodeErr := newConnError(c.endpoint.NodeID(), c.endpoint.Address(), err)
	return xerrors.WithStackTrace(nodeErr, xerrors.WithSkipDepth(1))
}

type option func(c *conn)

func withOnClose(onClose func(*conn)) option {
	return func(c *conn) {
		if onClose != nil {
			c.onClose = append(c.onClose, onClose)
		}
	}
}

func withOnTransportError(onTransportError func(ctx context.Context, cc Conn, cause error)) option {
	return func(c *conn) {
		if onTransportError != nil {
			c.onTransportErrors = append(c.onTransportErrors, onTransportError)
		}
	}
}

func newConn(e endpoint.Endpoint, config Config, opts ...option) *conn {
	c := &conn{
		endpoint: e,
		config:   config,
		done:     make(chan struct{}),
	}
	c.state.Store(uint32(Created))
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}

	return c
}

func New(e endpoint.Endpoint, config Config, opts ...option) Conn {
	return newConn(e, config, opts...)
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
	return v.(*modificationMark)
}

type modificationMark struct {
	dirty xatomic.Bool
}

func (m *modificationMark) canRetry() bool {
	return !m.dirty.Load()
}

func (m *modificationMark) markDirty() {
	m.dirty.Store(true)
}
