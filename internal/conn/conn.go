package conn

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/response"
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
	SetState(State) State
	Unban() State
}

type conn struct {
	mtx               sync.RWMutex
	config            Config // ro access
	cc                *grpc.ClientConn
	done              chan struct{}
	endpoint          endpoint.Endpoint // ro access
	closed            bool
	state             uint32
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
	state := State(atomic.LoadUint32(&c.state))
	for _, s := range states {
		if s == state {
			return true
		}
	}

	return false
}

func (c *conn) park(ctx context.Context) (err error) {
	onDone := trace.DriverOnConnPark(
		c.config.Trace(),
		&ctx,
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

	err = c.close()

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

func (c *conn) SetState(s State) State {
	return c.setState(s)
}

func (c *conn) setState(s State) State {
	state := atomic.LoadUint32(&c.state)
	if atomic.CompareAndSwapUint32(&c.state, state, uint32(s)) {
		trace.DriverOnConnStateChange(
			c.config.Trace(),
			c.endpoint.Copy(),
			State(state),
		)(s)
	}
	return s
}

func (c *conn) Unban() State {
	var newState State
	if isAvailable(c.cc) {
		newState = Online
	} else {
		newState = Offline
	}

	c.setState(newState)
	return newState
}

func (c *conn) GetState() (s State) {
	return State(atomic.LoadUint32(&c.state))
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
		c.config.Trace(),
		&ctx,
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
	c.setState(Online)

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
func (c *conn) close() (err error) {
	if c.cc == nil {
		return nil
	}
	err = c.cc.Close()
	c.cc = nil
	c.setState(Offline)
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
		c.config.Trace(),
		&ctx,
		c.Endpoint(),
	)
	defer func() {
		onDone(err)
	}()

	c.closed = true

	err = c.close()

	c.setState(Destroyed)

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
			c.config.Trace(),
			&ctx,
			c.endpoint,
			trace.Method(method),
		)
		cc *grpc.ClientConn
		md = metadata.MD{}
	)

	defer func() {
		onDone(err, issues, opID, c.GetState(), md)
	}()

	defer func() {
		meta.CallTrailerCallback(ctx, md)
	}()

	cc, err = c.realConn(ctx)
	if err != nil {
		return c.wrapError(err)
	}

	c.touchLastUsage()
	defer c.touchLastUsage()

	traceID, err := uuid.NewUUID()
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	ctx, sentMark := markContext(meta.WithTraceID(ctx, traceID.String()))

	err = cc.Invoke(ctx, method, req, res, append(opts, grpc.Trailer(&md))...)
	if err != nil {
		defer func() {
			c.onTransportError(ctx, err)
		}()

		if useWrapping {
			err = xerrors.Transport(err,
				xerrors.WithAddress(c.Address()),
				xerrors.WithTraceID(traceID.String()),
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
						xerrors.WithTraceID(traceID.String()),
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
			c.config.Trace(),
			&ctx,
			c.endpoint.Copy(),
			trace.Method(method),
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

	traceID, err := uuid.NewUUID()
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	ctx, sentMark := markContext(meta.WithTraceID(ctx, traceID.String()))

	s, err = cc.NewStream(ctx, desc, method, opts...)
	if err != nil {
		defer func() {
			c.onTransportError(ctx, err)
		}()

		if useWrapping {
			err = xerrors.Transport(err,
				xerrors.WithAddress(c.Address()),
				xerrors.WithTraceID(traceID.String()),
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
	nodeErr := newNodeError(c.endpoint.NodeID(), c.endpoint.Address(), err)
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
		state:    uint32(Created),
		endpoint: e,
		config:   config,
		done:     make(chan struct{}),
	}
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
	dirty uint32
}

func (m *modificationMark) canRetry() bool {
	return atomic.LoadUint32(&m.dirty) == 0
}

func (m *modificationMark) markDirty() {
	atomic.StoreUint32(&m.dirty, 1)
}
