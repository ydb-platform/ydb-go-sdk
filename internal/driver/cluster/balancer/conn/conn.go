package conn

import (
	"context"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/response"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Conn interface {
	grpc.ClientConnInterface

	Address() string
	Runtime() runtime.Runtime
	Close(ctx context.Context) error
	Location() trace.Location
}

func (c *conn) Address() string {
	return c.address
}

type conn struct {
	sync.Mutex

	dial    func(context.Context, string) (*grpc.ClientConn, error)
	address string
	runtime runtime.Runtime
	done    chan struct{}
	closed  bool

	config Config

	inflight int32
	grpcConn *grpc.ClientConn
}

func (c *conn) Location() trace.Location {
	return c.runtime.Location()
}

func (c *conn) Runtime() runtime.Runtime {
	return c.runtime
}

func (c *conn) Take(ctx context.Context) (raw *grpc.ClientConn, err error) {
	onDone := trace.DriverOnConnTake(c.config.Trace(ctx), ctx, c.address, c.runtime.Location())
	defer func() {
		onDone(err)
	}()
	if c.isClosed() {
		return nil, errors.NewTransportError(errors.WithTEReason(errors.TransportErrorUnavailable))
	}
	c.Lock()
	defer c.Unlock()
	if isBroken(c.grpcConn) {
		_ = c.close(ctx)
		raw, err = c.dial(ctx, c.address)
		if err != nil {
			return nil, err
		}
		c.grpcConn = raw
		c.runtime.SetState(ctx, state.Online)
	}
	atomic.AddInt32(&c.inflight, 1)
	return c.grpcConn, nil
}

func (c *conn) release(ctx context.Context) {
	onDone := trace.DriverOnConnRelease(c.config.Trace(ctx), ctx, c.address, c.Location())
	defer onDone()
	atomic.AddInt32(&c.inflight, -1)
}

func isBroken(raw *grpc.ClientConn) bool {
	if raw == nil {
		return true
	}
	s := raw.GetState()
	return s == connectivity.Shutdown || s == connectivity.TransientFailure
}

func (c *conn) IsReady() bool {
	if c == nil {
		return false
	}
	c.Lock()
	defer c.Unlock()
	return c.grpcConn != nil && c.grpcConn.GetState() == connectivity.Ready
}

func (c *conn) close(ctx context.Context) (err error) {
	if c.grpcConn == nil {
		return nil
	}
	onDone := trace.DriverOnConnDisconnect(c.config.Trace(ctx), ctx, c.address, c.runtime.Location(), c.runtime.GetState())
	err = c.grpcConn.Close()
	c.grpcConn = nil
	c.runtime.SetState(ctx, state.Offline)
	onDone(c.runtime.GetState(), err)
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
	onDone := trace.DriverOnConnClose(c.config.Trace(ctx), ctx, c.address, c.Location(), c.runtime.GetState())
	defer func() {
		c.runtime.SetState(ctx, state.Unknown)
		onDone()
	}()
	c.closed = true
	return c.close(ctx)
}

func (c *conn) pessimize(ctx context.Context, err error) {
	c.Lock()
	if c.closed {
		c.Unlock()
		return
	}
	if c.runtime.GetState() == state.Banned {
		c.Unlock()
		return
	}
	onDone := trace.DriverOnPessimizeNode(c.config.Trace(ctx), ctx, c.address, c.runtime.Location(), c.runtime.GetState(), err)
	err = c.config.Pessimize(ctx, c.address)
	c.runtime.SetState(ctx, state.Banned)
	onDone(c.runtime.GetState(), err)
	c.Unlock()
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
	onDone := trace.DriverOnConnInvoke(c.config.Trace(ctx), rawCtx, c.address, c.runtime.Location(), trace.Method(method))
	defer func() {
		onDone(err, issues, opID, c.runtime.GetState())
		c.runtime.OperationDone(start, timeutil.Now(), err)
	}()

	var cc *grpc.ClientConn
	cc, err = c.Take(ctx)
	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}
	defer c.release(ctx)

	err = cc.Invoke(ctx, method, req, res, opts...)

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
	streamRecv := trace.DriverOnConnNewStream(c.config.Trace(ctx), rawCtx, c.address, c.runtime.Location(), trace.Method(method))
	defer func() {
		if err != nil {
			c.runtime.StreamDone(timeutil.Now(), err)
			streamRecv(err)(c.runtime.GetState(), err)
			c.release(ctx)
		}
	}()

	var cc *grpc.ClientConn
	cc, err = c.Take(ctx)
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
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return nil, err
	}

	return &grpcClientStream{
		ctx:    rawCtx,
		c:      c,
		s:      s,
		cancel: cancel,
		recv:   streamRecv,
	}, nil
}

func New(ctx context.Context, address string, location trace.Location, dial func(context.Context, string) (*grpc.ClientConn, error), config Config) Conn {
	onDone := trace.DriverOnConnNew(config.Trace(ctx), ctx, address, location)
	c := &conn{
		address: address,
		dial: func(ctx context.Context, s string) (*grpc.ClientConn, error) {
			onDone := trace.DriverOnConnDial(config.Trace(ctx), ctx, address, location)
			raw, err := dial(ctx, address)
			defer func() {
				onDone(err)
			}()
			return raw, err
		},
		config:  config,
		done:    make(chan struct{}),
		runtime: runtime.New(config.Trace(ctx), address, location),
	}
	defer func() {
		onDone()
	}()
	return c
}
