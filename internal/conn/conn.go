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

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
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
)

type (
	Info interface {
		endpoint.Info

		State() State
		Ready() bool
	}
	Conn interface {
		grpc.ClientConnInterface
		Info
	}
)

type lazyConn struct {
	config   Config        // ro access
	endpoint endpoint.Info // ro access

	cc *ccGuard

	inUse inUseGuard

	childStreams *xcontext.CancelsGuard

	lastUsage xsync.LastUsage

	onClose []func(*lazyConn)
}

func (c *lazyConn) String() string {
	return c.endpoint.String()
}

func (c *lazyConn) Location() string {
	return c.endpoint.Location()
}

func (c *lazyConn) LastUpdated() time.Time {
	return c.endpoint.LastUpdated()
}

func (c *lazyConn) LoadFactor() float32 {
	return c.endpoint.LoadFactor()
}

func (c *lazyConn) Address() string {
	return c.endpoint.Address()
}

func (c *lazyConn) NodeID() uint32 {
	return c.endpoint.NodeID()
}

func (c *lazyConn) park(ctx context.Context) (finalErr error) {
	onDone := trace.DriverOnConnPark(
		c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*lazyConn).park"),
		c.Endpoint(),
	)
	defer func() {
		onDone(finalErr)
	}()

	locked, unlock := c.inUse.TryLock()
	if !locked {
		return nil
	}
	defer unlock()

	err := c.cc.Close(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *lazyConn) Endpoint() endpoint.Info {
	if c != nil {
		return c.endpoint
	}

	return nil
}

func (c *lazyConn) Ready() bool {
	return Ready(c.cc.State())
}

func (c *lazyConn) State() State {
	return c.cc.State()
}

func (c *lazyConn) Close(ctx context.Context) (finalErr error) {
	onDone := trace.DriverOnConnClose(
		c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*lazyConn).Close"),
		c.Endpoint(),
	)
	defer func() {
		onDone(finalErr)
	}()

	defer func() {
		for _, onClose := range c.onClose {
			onClose(c)
		}
		c.inUse.Stop()
	}()

	err := c.cc.Close(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *lazyConn) Invoke(
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
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*lazyConn).Invoke"),
			c.endpoint, trace.Method(method),
		)
		cc *grpc.ClientConn
		md = metadata.MD{}
	)
	defer func() {
		meta.CallTrailerCallback(ctx, md)
		onDone(err, issues, opID, c.State(), md)
	}()

	locked, unlock := c.inUse.TryLock()
	if !locked {
		return xerrors.WithStackTrace(errClosedConnection)
	}
	defer unlock()

	cc, err = c.cc.Get(ctx)
	if err != nil {
		return c.wrapError(err)
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
		if xerrors.IsTransportError(err) && useWrapping {
			err = xerrors.Transport(err,
				xerrors.WithAddress(c.Address()),
				xerrors.WithTraceID(traceID),
			)
			if sentMark.canRetry() {
				return c.wrapError(xerrors.Retryable(err, xerrors.WithName("Invoke")))
			}

			return c.wrapError(err)
		}

		return xerrors.WithStackTrace(err)
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

//nolint:funlen
func (c *lazyConn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (_ grpc.ClientStream, finalErr error) {
	var (
		onDone = trace.DriverOnConnNewStream(
			c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*lazyConn).NewStream"),
			c.endpoint, trace.Method(method),
		)
		useWrapping = UseWrapping(ctx)
	)

	defer func() {
		onDone(finalErr, c.State())
	}()

	locked, unlock := c.inUse.TryLock()
	if !locked {
		return nil, xerrors.WithStackTrace(errClosedConnection)
	}
	defer unlock()

	cc, err := c.cc.Get(ctx)
	if err != nil {
		return nil, c.wrapError(err)
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
		if xerrors.IsTransportError(err) && useWrapping {
			err = xerrors.Transport(err,
				xerrors.WithAddress(c.Address()),
				xerrors.WithTraceID(traceID),
			)
			if sentMark.canRetry() {
				return s, c.wrapError(xerrors.Retryable(err, xerrors.WithName("NewStream")))
			}

			return s, c.wrapError(err)
		}

		return nil, xerrors.WithStackTrace(err)
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

func (c *lazyConn) wrapError(err error) error {
	if err == nil {
		return nil
	}
	nodeErr := newConnError(c.endpoint.NodeID(), c.endpoint.Address(), err)

	return xerrors.WithStackTrace(nodeErr, xerrors.WithSkipDepth(1))
}

type option func(c *lazyConn)

func withOnClose(onClose func(*lazyConn)) option {
	return func(c *lazyConn) {
		if onClose != nil {
			c.onClose = append(c.onClose, onClose)
		}
	}
}

func dial(
	ctx context.Context,
	t *trace.Driver,
	e endpoint.Info,
	opts ...grpc.DialOption,
) (_ *grpc.ClientConn, finalErr error) {
	onDone := trace.DriverOnConnDial(
		t, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.dial"),
		e,
	)
	defer func() {
		onDone(finalErr)
	}()

	// prepend "ydb" scheme for grpc dns-resolver to find the proper scheme
	// three slashes in "ydb:///" is ok. It needs for good parse scheme in grpc resolver.
	address := "ydb:///" + e.Address()

	cc, err := grpc.DialContext(ctx, address, append(
		[]grpc.DialOption{
			grpc.WithStatsHandler(statsHandler{}),
		}, opts...,
	)...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return cc, nil
}

func newConn(e endpoint.Info, config Config, opts ...option) *lazyConn {
	c := &lazyConn{
		endpoint:     e,
		config:       config,
		lastUsage:    xsync.NewLastUsage(),
		childStreams: xcontext.NewCancelsGuard(),
		onClose: []func(*lazyConn){
			func(c *lazyConn) {
				c.childStreams.Cancel()
				c.inUse.Stop()
			},
		},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	c.cc = newCcGuard(func(ctx context.Context) (*grpc.ClientConn, error) {
		if dialTimeout := c.config.DialTimeout(); dialTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = xcontext.WithTimeout(ctx, dialTimeout)
			defer cancel()
		}

		cc, err := dial(ctx, c.config.Trace(), c.endpoint, c.config.GrpcDialOptions()...)
		if err != nil {
			if xerrors.IsTransportError(err) {
				return nil, xerrors.WithStackTrace(
					xerrors.Retryable(
						xerrors.Transport(err,
							xerrors.WithAddress(c.endpoint.Address()),
						),
					),
				)
			}

			return nil, xerrors.WithStackTrace(
				xerrors.Retryable(err, xerrors.WithName("dial")),
			)
		}

		return cc, nil
	})

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
