package conn

import (
	"context"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/response"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Conn interface {
	grpc.ClientConnInterface

	Endpoint() endpoint.Endpoint
	GetState() state.State
	SetState(context.Context, state.State) state.State
	Close(ctx context.Context) error
}

func (c *conn) Address() string {
	return c.endpoint.Address()
}

type conn struct {
	sync.Mutex

	dial     func(context.Context, string) (*grpc.ClientConn, error)
	endpoint endpoint.Endpoint // ro access
	done     chan struct{}
	closed   bool

	config Config // ro access

	cc    *grpc.ClientConn
	state state.State
	locks int32
}

func (c *conn) NodeID() uint32 {
	if c != nil {
		return c.endpoint.ID
	}
	return 0
}

func (c *conn) Endpoint() endpoint.Endpoint {
	if c != nil {
		return c.endpoint
	}
	return endpoint.Endpoint{}
}

func (c *conn) SetState(ctx context.Context, s state.State) state.State {
	c.Lock()
	defer c.Unlock()
	return c.setState(ctx, s)
}

func (c *conn) setState(ctx context.Context, s state.State) state.State {
	onDone := trace.DriverOnConnStateChange(c.config.Trace(ctx), ctx, c.endpoint, c.state)
	c.state = s
	onDone(c.state)
	return c.state
}

func (c *conn) GetState() (s state.State) {
	c.Lock()
	defer c.Unlock()
	return c.state
}

func (c *conn) take(ctx context.Context) (cc *grpc.ClientConn, err error) {
	onDone := trace.DriverOnConnTake(c.config.Trace(ctx), ctx, c.endpoint)
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
		cc, err = c.dial(ctx, c.endpoint.Address())
		if err != nil {
			return nil, err
		}
		c.cc = cc
		c.setState(ctx, state.Online)
	}
	atomic.AddInt32(&c.locks, 1)
	return c.cc, nil
}

func (c *conn) release(ctx context.Context) {
	c.Lock()
	defer c.Unlock()
	onDone := trace.DriverOnConnRelease(c.config.Trace(ctx), ctx, c.endpoint)
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

func (c *conn) close(ctx context.Context) (err error) {
	if c.cc == nil {
		return nil
	}
	err = c.cc.Close()
	c.cc = nil
	c.setState(ctx, state.Offline)
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
	c.setState(ctx, state.Destroyed)
	return err
}

func (c *conn) pessimize(ctx context.Context, err error) {
	c.Lock()
	if c.closed {
		c.Unlock()
		return
	}
	if c.state == state.Banned {
		c.Unlock()
		return
	}
	c.Unlock()
	trace.DriverOnPessimizeNode(
		c.config.Trace(ctx),
		ctx,
		c.endpoint,
		c.GetState(),
		err,
	)(
		c.SetState(ctx, state.Banned),
		c.config.Pessimize(ctx, c.endpoint),
	)
}

func (c *conn) Invoke(ctx context.Context, method string, req interface{}, res interface{}, opts ...grpc.CallOption) (err error) {
	if c.isClosed() {
		return errors.NewTransportError(errors.WithTEReason(errors.TransportErrorUnavailable))
	}
	var (
		rawCtx = ctx
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

	ctx, err = c.config.Meta(ctx)
	if err != nil {
		return err
	}

	onDone := trace.DriverOnConnInvoke(c.config.Trace(ctx), rawCtx, c.endpoint, trace.Method(method))
	defer func() {
		onDone(err, issues, opID, c.GetState())
	}()

	var cc *grpc.ClientConn
	cc, err = c.take(ctx)
	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}

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

	return
}

func (c *conn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (_ grpc.ClientStream, err error) {
	if c.isClosed() {
		return nil, errors.NewTransportError(errors.WithTEReason(errors.TransportErrorUnavailable))
	}

	// Remember raw deadline to pass it for the tracing functions.
	rawCtx := ctx

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

	ctx, err = c.config.Meta(ctx)
	if err != nil {
		return nil, err
	}

	streamRecv := trace.DriverOnConnNewStream(c.config.Trace(ctx), rawCtx, c.endpoint, trace.Method(method))
	defer func() {
		if err != nil {
			streamRecv(err)(c.GetState(), err)
		}
	}()

	var cc *grpc.ClientConn
	cc, err = c.take(ctx)
	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}

	var s grpc.ClientStream
	s, err = cc.NewStream(ctx, desc, method, append(opts, grpc.MaxCallRecvMsgSize(50*1024*1024))...)
	if err != nil {
		c.release(ctx)
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return nil, err
	}

	return &grpcClientStream{
		c: c,
		s: s,
		onDone: func(ctx context.Context) {
			c.release(ctx)
			cancel()
		},
		recv: streamRecv,
	}, nil
}

func New(endpoint endpoint.Endpoint, dial func(context.Context, string) (*grpc.ClientConn, error), config Config) Conn {
	c := &conn{
		endpoint: endpoint,
		dial:     dial,
		config:   config,
		done:     make(chan struct{}),
	}
	return c
}
