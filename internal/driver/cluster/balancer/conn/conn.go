package conn

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/response"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Conn interface {
	grpc.ClientConnInterface

	Addr() endpoint.Addr
	Runtime() runtime.Runtime
	Close() error
}

func (c *conn) isNil() bool {
	return c == nil
}

func (c *conn) Address() string {
	return c.Addr().String()
}

type conn struct {
	sync.Mutex

	dial    func(context.Context, string, int) (*grpc.ClientConn, error)
	addr    endpoint.Addr
	runtime runtime.Runtime
	done    chan struct{}

	config Config

	timer    timeutil.Timer
	grpcConn *grpc.ClientConn
}

func (c *conn) Addr() endpoint.Addr {
	return c.addr
}

func (c *conn) Runtime() runtime.Runtime {
	return c.runtime
}

func (c *conn) Conn(ctx context.Context) (*grpc.ClientConn, error) {
	c.Lock()
	defer c.Unlock()
	if c.grpcConn == nil || isBroken(c.grpcConn) {
		onDone := trace.DriverOnConnDial(c.config.Trace(ctx), ctx, c.addr, c.runtime.GetState())
		raw, err := c.dial(ctx, c.addr.Host, c.addr.Port)
		defer func() {
			onDone(err, c.runtime.GetState())
		}()
		if err != nil {
			return nil, err
		}
		c.grpcConn = raw
		if c.runtime.GetState() != state.Banned {
			c.runtime.SetState(state.Online)
		}
	}
	if c.config.GrpcConnectionPolicy().TTL > 0 {
		c.timer.Reset(c.config.GrpcConnectionPolicy().TTL)
	}
	return c.grpcConn, nil
}

func isBroken(raw *grpc.ClientConn) bool {
	if raw == nil {
		return true
	}
	state := raw.GetState()
	return state == connectivity.Shutdown || state == connectivity.TransientFailure
}

func (c *conn) IsReady() bool {
	if c == nil {
		return false
	}
	c.Lock()
	defer c.Unlock()
	return c.grpcConn != nil && c.grpcConn.GetState() == connectivity.Ready
}

func (c *conn) waitClose() {
	if c.config.GrpcConnectionPolicy().TTL <= 0 {
		return
	}
	c.timer.Reset(c.config.GrpcConnectionPolicy().TTL)
	for {
		select {
		case <-c.done:
			return
		case <-c.timer.C():
			c.Lock()
			if c.grpcConn != nil {
				_ = c.close()
			}
			c.timer.Reset(time.Duration(math.MaxInt64))
			c.Unlock()
		}
	}
}

// c mutex must be locked
func (c *conn) close() (err error) {
	onDone := trace.DriverOnConnDisconnect(c.config.Trace(context.Background()), c.addr, c.runtime.GetState())
	err = c.grpcConn.Close()
	c.runtime.SetState(state.Offline)
	onDone(err, c.runtime.GetState())
	c.grpcConn = nil
	return err
}

func (c *conn) Close() (err error) {
	c.Lock()
	defer c.Unlock()
	if c.done == nil {
		return nil
	}
	if !c.timer.Stop() {
		panic(fmt.Errorf("cant stop timer for conn to '%v'", c.addr.String()))
	}
	if c.done != nil {
		close(c.done)
		c.done = nil
	}
	if c.grpcConn != nil {
		_ = c.close()
	}
	trace.DriverOnConnClose(c.config.Trace(context.Background()), c.addr, c.runtime.GetState())
	return err
}

func (c *conn) pessimize(ctx context.Context, err error) {
	c.Lock()
	defer c.Unlock()
	if c.runtime.Stats().State == state.Banned {
		return
	}
	onDone := trace.DriverOnPessimizeNode(c.config.Trace(ctx), ctx, c.Addr(), c.runtime.Stats().State, err)
	err = c.config.Pessimize(c.addr)
	c.runtime.SetState(state.Banned)
	onDone(state.Banned, err)
}

func (c *conn) Invoke(ctx context.Context, method string, req interface{}, res interface{}, opts ...grpc.CallOption) (err error) {
	var (
		rawCtx = ctx
		cancel context.CancelFunc
		opId   string
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

	start := timeutil.Now()
	c.runtime.OperationStart(start)
	onDone := trace.DriverOnConnInvoke(c.config.Trace(ctx), rawCtx, c.Addr(), trace.Method(method))
	defer func() {
		onDone(err, issues, opId)
		err = errors.ErrIf(errors.IsTimeoutError(err), err)
		c.runtime.OperationDone(start, timeutil.Now(), err)
	}()

	ctx, err = c.config.Meta(ctx)
	if err != nil {
		return err
	}

	raw, err := c.Conn(ctx)
	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}

	err = raw.Invoke(ctx, method, req, res, opts...)

	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}
	if opResponse, ok := res.(response.OpResponse); ok {
		opId = opResponse.GetOperation().GetId()
		for _, issue := range opResponse.GetOperation().GetIssues() {
			issues = append(issues, issue)
		}
		switch {
		case !opResponse.GetOperation().GetReady():
			err = errors.ErrOperationNotReady

		case opResponse.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
			err = errors.NewOpError(errors.WithOEOperation(opResponse.GetOperation()))
		}
	}

	return
}

func (c *conn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (_ grpc.ClientStream, err error) {
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
	}

	c.runtime.StreamStart(timeutil.Now())
	streamRecv := trace.DriverOnConnNewStream(c.config.Trace(ctx), rawCtx, c.Addr(), trace.Method(method))
	defer func() {
		if err != nil {
			c.runtime.StreamDone(timeutil.Now(), err)
		}
	}()

	ctx, err = c.config.Meta(ctx)
	if err != nil {
		return nil, err
	}

	raw, err := c.Conn(ctx)
	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}

	s, err := raw.NewStream(ctx, desc, method, append(opts, grpc.MaxCallRecvMsgSize(50*1024*1024))...)
	if err != nil {
		return nil, errors.MapGRPCError(err)
	}

	return &grpcClientStream{
		ctx:    rawCtx,
		c:      c,
		s:      s,
		cancel: cancel,
		recv:   streamRecv,
	}, nil
}

func New(ctx context.Context, addr endpoint.Addr, dial func(context.Context, string, int) (*grpc.ClientConn, error), cfg Config) Conn {
	c := &conn{
		addr:    addr,
		dial:    dial,
		config:  cfg,
		timer:   timeutil.NewTimer(time.Duration(math.MaxInt64)),
		done:    make(chan struct{}),
		runtime: runtime.New(cfg.Trace(ctx), addr),
	}
	go c.waitClose()
	trace.DriverOnConnNew(cfg.Trace(ctx), addr, c.runtime.GetState())
	return c
}
