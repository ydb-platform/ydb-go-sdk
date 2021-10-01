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

	Endpoint() endpoint.Endpoint
	Runtime() runtime.Runtime
	Close()
}

func (c *conn) Address() string {
	return c.endpoint.Addr.String()
}

type conn struct {
	sync.Mutex

	dial      func(context.Context, string, int) (*grpc.ClientConn, error)
	endpoint  endpoint.Endpoint
	runtime   runtime.Runtime
	done      chan struct{}
	closeOnce sync.Once

	config Config

	timer    timeutil.Timer
	grpcConn *grpc.ClientConn
}

func (c *conn) Endpoint() endpoint.Endpoint {
	return c.endpoint
}

func (c *conn) Runtime() runtime.Runtime {
	return c.runtime
}

func (c *conn) Conn(ctx context.Context) (*grpc.ClientConn, error) {
	c.Lock()
	defer c.Unlock()
	if c.grpcConn == nil || isBroken(c.grpcConn) {
		onDone := trace.DriverOnConnDial(c.config.Trace(ctx), ctx, c.endpoint, c.runtime.GetState())
		raw, err := c.dial(ctx, c.endpoint.Host, c.endpoint.Port)
		defer func() {
			onDone(err, c.runtime.GetState())
		}()
		if err != nil {
			return nil, err
		}
		c.grpcConn = raw
		c.runtime.SetState(state.Online)
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

func (c *conn) resetTimer() {
	if c.config.GrpcConnectionPolicy().TTL > 0 {
		c.timer.Reset(c.config.GrpcConnectionPolicy().TTL)
	} else {
		c.timer.Reset(time.Duration(math.MaxInt64))
	}
}

func (c *conn) waitClose() {
	defer c.close()
	c.resetTimer()
	for {
		select {
		case <-c.done:
			return
		case <-c.timer.C():
			if !c.close() {
				c.resetTimer()
			}
		}
	}
}

// c mutex must be unlocked
func (c *conn) close() bool {
	c.Lock()
	defer c.Unlock()
	if c.grpcConn == nil {
		return false
	}
	onDone := trace.DriverOnConnDisconnect(c.config.Trace(context.Background()), c.endpoint, c.runtime.GetState())
	c.grpcConn.Close()
	c.grpcConn = nil
	c.runtime.SetState(state.Offline)
	c.timer.Reset(time.Duration(math.MaxInt64))
	onDone(c.runtime.GetState())
	return true
}

func (c *conn) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		if !c.timer.Stop() {
			panic(fmt.Errorf("cant stop timer for conn to '%v'", c.Address()))
		}
		trace.DriverOnConnClose(c.config.Trace(context.Background()), c.endpoint, c.runtime.GetState())
	})
}

func (c *conn) pessimize(ctx context.Context, err error) {
	c.Lock()
	defer c.Unlock()
	if c.runtime.Stats().State == state.Banned {
		return
	}
	onDone := trace.DriverOnPessimizeNode(c.config.Trace(ctx), ctx, c.endpoint, c.runtime.Stats().State, err)
	err = c.config.Pessimize(c.endpoint.Addr)
	c.runtime.SetState(state.Banned)
	onDone(state.Banned, err)
	go c.close()
}

func (c *conn) Invoke(ctx context.Context, method string, req interface{}, res interface{}, opts ...grpc.CallOption) (err error) {
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

	start := timeutil.Now()
	c.runtime.OperationStart(start)
	onDone := trace.DriverOnConnInvoke(c.config.Trace(ctx), rawCtx, c.endpoint, trace.Method(method))
	defer func() {
		onDone(err, issues, opID)
		c.runtime.OperationDone(start, timeutil.Now(), err)
	}()

	ctx, err = c.config.Meta(ctx)
	if err != nil {
		return err
	}

	var raw *grpc.ClientConn
	raw, err = c.Conn(ctx)
	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}

	err = raw.Invoke(ctx, method, req, res, opts...)
	c.resetTimer()

	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}
	if operation, ok := res.(response.Response); ok {
		opID = operation.GetOperation().GetId()
		for _, issue := range operation.GetOperation().GetIssues() {
			issues = append(issues, issue)
		}
		switch {
		case !operation.GetOperation().GetReady():
			return errors.ErrOperationNotReady

		case operation.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
			return errors.NewOpError(errors.WithOEOperation(operation.GetOperation()))
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
	streamRecv := trace.DriverOnConnNewStream(c.config.Trace(ctx), rawCtx, c.endpoint, trace.Method(method))
	defer func() {
		if err != nil {
			c.runtime.StreamDone(timeutil.Now(), err)
		}
	}()

	ctx, err = c.config.Meta(ctx)
	if err != nil {
		return nil, err
	}

	var raw *grpc.ClientConn
	raw, err = c.Conn(ctx)
	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}

	var s grpc.ClientStream
	s, err = raw.NewStream(ctx, desc, method, append(opts, grpc.MaxCallRecvMsgSize(50*1024*1024))...)
	if err != nil {
		return nil, errors.MapGRPCError(err)
	}

	c.resetTimer()

	return &grpcClientStream{
		ctx:    rawCtx,
		c:      c,
		s:      s,
		cancel: cancel,
		recv:   streamRecv,
	}, nil
}

func New(ctx context.Context, endpoint endpoint.Endpoint, dial func(context.Context, string, int) (*grpc.ClientConn, error), cfg Config) Conn {
	c := &conn{
		endpoint: endpoint,
		dial:     dial,
		config:   cfg,
		timer:    timeutil.NewTimer(time.Duration(math.MaxInt64)),
		done:     make(chan struct{}),
		runtime:  runtime.New(cfg.Trace(ctx), endpoint),
	}
	go c.waitClose()
	trace.DriverOnConnNew(cfg.Trace(ctx), endpoint, c.runtime.GetState())
	return c
}
