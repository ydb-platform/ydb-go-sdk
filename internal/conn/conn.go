package conn

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/response"
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

	Address() string

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
	address           string
	closed            bool
	state             atomic.Uint32
	childStreams      *xcontext.CancelsGuard
	lastUsage         xsync.LastUsage
	onClose           []func(*conn)
	onTransportErrors []func(ctx context.Context, cc Conn, cause error)
}

func (c *conn) Address() string {
	return c.address
}

func (c *conn) Ping(ctx context.Context) error {
	cc, err := c.realConn(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	if !isAvailable(cc) {
		return xerrors.WithStackTrace(errUnavailableConnection)
	}

	return nil
}

func (c *conn) LastUsage() time.Time {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	return c.lastUsage.Get()
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
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*conn).park"),
		c.address,
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
		return xerrors.WithStackTrace(err)
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
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*conn).setState"),
			c.address, state,
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
		return nil, xerrors.WithStackTrace(errClosedConnection)
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
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*conn).realConn"),
		c.address,
	)
	defer func() {
		onDone(err)
	}()

	// prepend "ydb" scheme for grpc dns-resolver to find the proper scheme
	// three slashes in "ydb:///" is ok. It needs for good parse scheme in grpc resolver.
	address := "ydb:///" + c.address

	cc, err = grpc.DialContext(ctx, address, append(
		[]grpc.DialOption{
			grpc.WithStatsHandler(statsHandler{}),
		}, c.config.GrpcDialOptions()...,
	)...)
	if err != nil {
		if xerrors.IsContextError(err) {
			return nil, xerrors.WithStackTrace(err)
		}

		defer func() {
			c.onTransportError(ctx, err)
		}()

		err = xerrors.Transport(err,
			xerrors.WithAddress(address),
		)

		return nil, xerrors.WithStackTrace(
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
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*conn).Close"),
		c.address,
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

	return xerrors.WithStackTrace(err)
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
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*conn).Invoke"),
			c.address, trace.Method(method),
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
		return xerrors.WithStackTrace(err)
	}

	stop := c.lastUsage.Start()
	defer stop()

	ctx, traceID, err := meta.TraceID(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	ctx, sentMark := markContext(meta.WithTraceID(ctx, traceID))

	err = cc.Invoke(ctx, method, req, res, append(opts, grpc.Trailer(&md))...)
	if err != nil {
		if xerrors.IsContextError(err) {
			return xerrors.WithStackTrace(err)
		}

		defer func() {
			c.onTransportError(ctx, err)
		}()

		if useWrapping {
			err = xerrors.Transport(err,
				xerrors.WithAddress(c.Address()),
				xerrors.WithTraceID(traceID),
			)
			if sentMark.canRetry() {
				return xerrors.WithStackTrace(xerrors.Retryable(err, xerrors.WithName("Invoke")))
			}

			return xerrors.WithStackTrace(err)
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
				return xerrors.WithStackTrace(errOperationNotReady)

			case o.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
				return xerrors.WithStackTrace(
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
			c.address, trace.Method(method),
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

	ctx, cancel := xcontext.WithCancel(ctx)
	defer func() {
		if finalErr != nil {
			cancel()
		} else {
			c.childStreams.Remember(&cancel)
		}
	}()

	s, err := cc.NewStream(ctx, desc, method, append(opts, grpc.OnFinish(func(err error) {
		cancel()
		c.childStreams.Forget(&cancel)
	}))...)
	if err != nil {
		if xerrors.IsContextError(err) {
			return nil, xerrors.WithStackTrace(err)
		}

		defer func() {
			c.onTransportError(ctx, err)
		}()

		if useWrapping {
			err = xerrors.Transport(err,
				xerrors.WithAddress(c.Address()),
				xerrors.WithTraceID(traceID),
			)
			if sentMark.canRetry() {
				return s, xerrors.WithStackTrace(xerrors.Retryable(err, xerrors.WithName("NewStream")))
			}

			return s, xerrors.WithStackTrace(err)
		}

		return s, err
	}

	return &grpcClientStream{
		ClientStream: s,
		ctx:          ctx,
		c:            c,
		wrapping:     useWrapping,
		traceID:      traceID,
		sentMark:     sentMark,
		onDone: func(ctx context.Context, md metadata.MD) {
			meta.CallTrailerCallback(ctx, md)
		},
	}, nil
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

func newConn(address string, config Config, opts ...option) *conn {
	c := &conn{
		address:      address,
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
	c.state.Store(uint32(Created))
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}

	return c
}

func New(address string, config Config, opts ...option) Conn {
	return newConn(address, config, opts...)
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
	dirty atomic.Bool
}

func (m *modificationMark) canRetry() bool {
	return !m.dirty.Load()
}

func (m *modificationMark) markDirty() {
	m.dirty.Store(true)
}
