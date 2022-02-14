package conn

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/response"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Conn interface {
	grpc.ClientConnInterface

	Endpoint() endpoint.Endpoint
	GetState() State
	SetState(context.Context, State) State
	Close(ctx context.Context) error
	Park(ctx context.Context) error
	TTL() <-chan time.Time
}

func (c *conn) Address() string {
	return c.endpoint.Address()
}

type conn struct {
	sync.Mutex
	config   Config // ro access
	cc       *grpc.ClientConn
	done     chan struct{}
	endpoint endpoint.Endpoint // ro access
	closed   bool
	state    State
	locks    int32
	ttl      timeutil.Timer
}

func (c *conn) Park(ctx context.Context) error {
	c.Lock()
	defer c.Unlock()
	return c.close(ctx)
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
	return endpoint.Endpoint{}
}

func (c *conn) SetState(ctx context.Context, s State) State {
	c.Lock()
	defer c.Unlock()
	return c.setState(ctx, s)
}

func (c *conn) setState(ctx context.Context, s State) State {
	onDone := trace.DriverOnConnStateChange(
		trace.ContextDriver(ctx).Compose(c.config.Trace()),
		&ctx,
		c.endpoint,
		c.state,
	)
	c.state = s
	onDone(c.state)
	return c.state
}

func (c *conn) GetState() (s State) {
	c.Lock()
	defer c.Unlock()
	return c.state
}

func (c *conn) TTL() <-chan time.Time {
	if c.config.ConnectionTTL() == 0 {
		return nil
	}
	if c.isClosed() {
		return nil
	}
	if atomic.LoadInt32(&c.locks) > 0 {
		return nil
	}
	return c.ttl.C()
}

func (c *conn) take(ctx context.Context) (cc *grpc.ClientConn, err error) {
	onDone := trace.DriverOnConnTake(
		trace.ContextDriver(ctx).Compose(c.config.Trace()),
		&ctx,
		c.endpoint,
	)
	defer func() {
		onDone(int(atomic.LoadInt32(&c.locks)), err)
	}()
	if c.isClosed() {
		return nil, errors.NewTransportError(errors.WithTEReason(errors.TransportErrorUnavailable))
	}
	c.Lock()
	defer c.Unlock()
	if isBroken(c.cc) {
		_ = c.close(ctx)
		if dialTimeout := c.config.DialTimeout(); dialTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, dialTimeout)
			defer cancel()
		}
		cc, err = grpc.DialContext(ctx, "ydb:///"+c.endpoint.Address(), c.config.GrpcDialOptions()...)
		if err != nil {
			return nil, err
		}
		c.cc = cc
		c.setState(ctx, Online)
	}
	atomic.AddInt32(&c.locks, 1)
	return c.cc, nil
}

func (c *conn) release(ctx context.Context) {
	c.Lock()
	defer c.Unlock()
	if ttl := c.config.ConnectionTTL(); ttl > 0 {
		c.ttl.Reset(ttl)
	}
	onDone := trace.DriverOnConnRelease(
		trace.ContextDriver(ctx).Compose(c.config.Trace()),
		&ctx,
		c.endpoint,
	)
	atomic.AddInt32(&c.locks, -1)
	onDone(int(atomic.LoadInt32(&c.locks)))
}

func isBroken(raw *grpc.ClientConn) bool {
	if raw == nil {
		return true
	}
	s := raw.GetState()
	return s == connectivity.Shutdown || s == connectivity.TransientFailure
}

// conn must be locked
func (c *conn) close(ctx context.Context) (err error) {
	if c.cc == nil {
		return nil
	}
	err = c.cc.Close()
	c.cc = nil
	c.setState(ctx, Offline)
	return err
}

func (c *conn) isClosed() bool {
	c.Lock()
	defer c.Unlock()
	return c.closed
}

func (c *conn) Close(ctx context.Context) (err error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	err = c.close(ctx)
	c.setState(ctx, Destroyed)
	return err
}

func (c *conn) pessimize(ctx context.Context, err error) {
	if c.isClosed() {
		return
	}
	trace.DriverOnPessimizeNode(
		trace.ContextDriver(ctx).Compose(c.config.Trace()),
		&ctx,
		c.endpoint,
		c.GetState(),
		err,
	)(c.SetState(ctx, Banned))
}

func (c *conn) Invoke(
	ctx context.Context,
	method string,
	req interface{},
	res interface{},
	opts ...grpc.CallOption,
) (err error) {
	if c.isClosed() {
		return errors.NewTransportError(errors.WithTEReason(errors.TransportErrorUnavailable))
	}

	var cc *grpc.ClientConn
	cc, err = c.take(ctx)
	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}

	var (
		cancel context.CancelFunc
		opID   string
		issues []trace.Issue
	)
	if t := c.config.RequestTimeout(); t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
	}
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	if t := c.config.OperationTimeout(); t > 0 {
		ctx = operation.WithTimeout(ctx, t)
	}
	if t := c.config.OperationCancelAfter(); t > 0 {
		ctx = operation.WithCancelAfter(ctx, t)
	}

	params := operation.ContextParams(ctx)
	if !params.Empty() {
		operation.SetOperationParams(req, params)
	}

	onDone := trace.DriverOnConnInvoke(
		trace.ContextDriver(ctx).Compose(c.config.Trace()),
		&ctx,
		c.endpoint,
		trace.Method(method),
	)
	defer func() {
		onDone(err, issues, opID, c.GetState())
	}()

	err = cc.Invoke(ctx, method, req, res, opts...)

	c.release(ctx)

	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}

	if o, ok := res.(response.Response); ok {
		opID = o.GetOperation().GetId()
		for _, issue := range o.GetOperation().GetIssues() {
			issues = append(issues, issue)
		}
		switch {
		case !o.GetOperation().GetReady():
			return errors.ErrOperationNotReady

		case o.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
			return errors.NewOpError(errors.WithOEOperation(o.GetOperation()))
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
	if c.isClosed() {
		return nil, errors.NewTransportError(errors.WithTEReason(errors.TransportErrorUnavailable))
	}

	var cc *grpc.ClientConn
	cc, err = c.take(ctx)
	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}

	var cancel context.CancelFunc
	if t := c.config.StreamTimeout(); t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
		defer func() {
			if err != nil {
				cancel()
			}
		}()
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	streamRecv := trace.DriverOnConnNewStream(
		trace.ContextDriver(ctx).Compose(c.config.Trace()),
		&ctx,
		c.endpoint,
		trace.Method(method),
	)
	defer func() {
		if err != nil {
			streamRecv(err)(c.GetState(), err)
		}
	}()

	var s grpc.ClientStream
	s, err = cc.NewStream(ctx, desc, method, append(opts, grpc.MaxCallRecvMsgSize(50*1024*1024))...)
	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		c.release(ctx)
		return nil, err
	}

	return &grpcClientStream{
		c: c,
		s: s,
		onDone: func(ctx context.Context) {
			cancel()
			c.release(ctx)
		},
		recv: streamRecv,
	}, nil
}

func New(endpoint endpoint.Endpoint, config Config) Conn {
	c := &conn{
		state:    Created,
		endpoint: endpoint,
		config:   config,
		done:     make(chan struct{}),
	}
	if ttl := config.ConnectionTTL(); ttl > 0 {
		c.ttl = timeutil.NewTimer(ttl)
	}
	return c
}
