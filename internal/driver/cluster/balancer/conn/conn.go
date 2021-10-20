package conn

import "C"

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/stats/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/response"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Conn interface {
	grpc.ClientConnInterface

	Endpoint() endpoint.Endpoint
	Runtime() runtime.Runtime
	Close(ctx context.Context) error
}

func (c *conn) Address() string {
	return c.endpoint.Address()
}

type conn struct {
	sync.Mutex

	dial     func(context.Context, string) (*grpc.ClientConn, error)
	endpoint endpoint.Endpoint
	runtime  runtime.Runtime
	done     chan struct{}
	closed   bool

	config Config

	cc *grpc.ClientConn
}

func (c *conn) Endpoint() endpoint.Endpoint {
	if c != nil {
		return c.endpoint
	}
	return endpoint.Endpoint{}
}

func (c *conn) Runtime() runtime.Runtime {
	return c.runtime
}

func (c *conn) take(ctx context.Context) (cc *grpc.ClientConn, err error) {
	if c.isClosed() {
		return nil, errors.NewTransportError(errors.WithTEReason(errors.TransportErrorUnavailable))
	}
	c.Lock()
	defer c.Unlock()
	//if isBroken(c.grpcConn) {
	if c.cc == nil {
		_ = c.close(ctx)
		cc, err = c.dial(ctx, c.endpoint.Address())
		if err != nil {
			return nil, err
		}
		c.cc = cc
		c.runtime.SetState(ctx, c.endpoint, state.Online)
	}
	c.runtime.Take(ctx, c.endpoint)
	return c.cc, nil
}

func (c *conn) release(ctx context.Context) {
	c.Lock()
	defer c.Unlock()
	c.runtime.Release(ctx, c.endpoint)
}

//nolint: deadcode
func isBroken(raw *grpc.ClientConn) bool {
	if raw == nil {
		return true
	}
	s := raw.GetState()
	return s == connectivity.Shutdown || s == connectivity.TransientFailure
}

//func (c *conn) IsReady() bool {
//	if c == nil {
//		return false
//	}
//	c.Lock()
//	defer c.Unlock()
//	return c.cc != nil && c.cc.GetState() == connectivity.Ready
//}
//
func (c *conn) close(ctx context.Context) (err error) {
	if c.cc == nil {
		return nil
	}
	err = c.cc.Close()
	c.cc = nil
	c.runtime.SetState(ctx, c.endpoint, state.Offline)
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
	defer c.runtime.SetState(ctx, c.endpoint, state.Unknown)
	c.closed = true
	return c.close(ctx)
}

func (c *conn) pessimize(ctx context.Context, err error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return
	}
	if c.runtime.GetState() == state.Banned {
		return
	}
	onDone := trace.DriverOnPessimizeNode(
		c.config.Trace(ctx),
		ctx,
		c.endpoint,
		c.runtime.GetState(),
		err,
	)
	onDone(
		c.runtime.SetState(ctx, c.endpoint, state.Banned),
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

	start := timeutil.Now()
	c.runtime.OperationStart(start)
	onDone := trace.DriverOnConnInvoke(c.config.Trace(ctx), rawCtx, c.endpoint, trace.Method(method))
	defer func() {
		onDone(err, issues, opID, c.runtime.GetState())
		c.runtime.OperationDone(start, timeutil.Now(), err)
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

	c.runtime.StreamStart(timeutil.Now())
	streamRecv := trace.DriverOnConnNewStream(c.config.Trace(ctx), rawCtx, c.endpoint, trace.Method(method))
	defer func() {
		if err != nil {
			c.runtime.StreamDone(timeutil.Now(), err)
			streamRecv(err)(c.runtime.GetState(), err)
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
		ctx: rawCtx,
		c:   c,
		s:   s,
		onDone: func(ctx context.Context) {
			c.release(ctx)
			cancel()
		},
		recv: streamRecv,
	}, nil
}

func New(ctx context.Context, endpoint endpoint.Endpoint, dial func(context.Context, string) (*grpc.ClientConn, error), config Config) Conn {
	c := &conn{
		endpoint: endpoint,
		dial:     dial,
		config:   config,
		done:     make(chan struct{}),
		runtime:  runtime.New(config.Trace(ctx)),
	}
	return c
}
