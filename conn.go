package ydb

import (
	"context"
	"google.golang.org/grpc/connectivity"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/timeutil"
)

type conn struct {
	dial    func(context.Context, string, int) (*grpc.ClientConn, error)
	addr    connAddr
	driver  *driver
	runtime *connRuntime
	done    chan struct{}

	mtx      *sync.Mutex
	timer    timeutil.Timer
	ttl      time.Duration
	grpcConn *grpc.ClientConn
}

func (c *conn) conn(ctx context.Context) (*grpc.ClientConn, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.grpcConn == nil || isBroken(c.grpcConn) {
		raw, err := c.dial(ctx, c.addr.addr, c.addr.port)
		if err != nil {
			return nil, err
		}
		c.grpcConn = raw
	}
	c.timer.Reset(c.ttl)
	return c.grpcConn, nil
}

func isBroken(raw *grpc.ClientConn) bool {
	if raw == nil {
		return true
	}
	state := raw.GetState()
	return state == connectivity.Shutdown || state == connectivity.TransientFailure
}

func (c *conn) isReady() bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c != nil && c.grpcConn != nil && c.grpcConn.GetState() == connectivity.Ready
}

func (c *conn) waitClose() {
	for {
		select {
		case <-c.done:
			return
		case <-c.timer.C():
			c.mtx.Lock()
			if c.grpcConn != nil {
				_ = c.grpcConn.Close()
				c.grpcConn = nil
			}
			c.mtx.Unlock()
		}
	}
}

func (c *conn) close() error {
	defer close(c.done)
	if !c.timer.Stop() {
		panic("cant stop timer")
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.grpcConn != nil {
		return c.grpcConn.Close()
	}
	return nil
}

func (c *conn) Invoke(ctx context.Context, method string, request interface{}, response interface{}, opts ...grpc.CallOption) (err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	var (
		cancel context.CancelFunc
		opId   string
		issues []*Ydb_Issue.IssueMessage
	)
	if t := c.driver.requestTimeout; t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
	}
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	if t := c.driver.operationTimeout; t > 0 {
		ctx = WithOperationTimeout(ctx, t)
	}
	if t := c.driver.operationCancelAfter; t > 0 {
		ctx = WithOperationCancelAfter(ctx, t)
	}

	// Get credentials (token actually) for the request.
	var md metadata.MD
	md, err = c.driver.meta.md(ctx)
	if err != nil {
		return
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	params := operationParams(ctx)
	if !params.Empty() {
		setOperationParams(request, params)
	}

	start := timeutil.Now()
	c.runtime.operationStart(start)
	operationDone := driverTraceOnOperation(c.driver.trace, ctx, c.Address(), Method(method), params)
	defer func() {
		operationDone(rawCtx, c.Address(), Method(method), params, opId, issues, err)
		c.runtime.operationDone(
			start, timeutil.Now(),
			errIf(isTimeoutError(err), err),
		)
	}()

	raw, err := c.conn(ctx)
	if err != nil {
		err = mapGRPCError(err)
		if te, ok := err.(*TransportError); ok && te.Reason != TransportErrorCanceled {
			// remove node from discovery cache on any transport error
			pessimizationDone := driverTraceOnPessimization(c.driver.trace, ctx, c.Address(), err)
			pessimizationDone(ctx, c.Address(), c.driver.cluster.Pessimize(c.addr))
		}
		return
	}

	err = raw.Invoke(ctx, method, request, response, opts...)

	if err != nil {
		err = mapGRPCError(err)
		if te, ok := err.(*TransportError); ok && te.Reason != TransportErrorCanceled {
			// remove node from discovery cache on any transport error
			pessimizationDone := driverTraceOnPessimization(c.driver.trace, ctx, c.Address(), err)
			pessimizationDone(ctx, c.Address(), c.driver.cluster.Pessimize(c.addr))
		}
		return
	}
	if operation, ok := response.(internal.OpResponse); ok {
		opId = operation.GetOperation().GetId()
		issues = operation.GetOperation().GetIssues()
		switch {
		case !operation.GetOperation().GetReady():
			err = ErrOperationNotReady

		case operation.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
			err = &OpError{
				Reason: statusCode(operation.GetOperation().GetStatus()),
				issues: operation.GetOperation().GetIssues(),
			}
		}
	}

	return
}

func (c *conn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (_ grpc.ClientStream, err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	var cancel context.CancelFunc
	if t := c.driver.streamTimeout; t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
		defer func() {
			if err != nil {
				cancel()
			}
		}()
	}

	// Get credentials (token actually) for the request.
	md, err := c.driver.meta.md(ctx)
	if err != nil {
		return
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	c.runtime.streamStart(timeutil.Now())
	streamRecv := driverTraceOnStream(c.driver.trace, ctx, c.Address(), Method(method))
	defer func() {
		if err != nil {
			c.runtime.streamDone(timeutil.Now(), err)
		}
	}()

	raw, err := c.conn(ctx)
	if err != nil {
		err = mapGRPCError(err)
		if te, ok := err.(*TransportError); ok && te.Reason != TransportErrorCanceled {
			// remove node from discovery cache on any transport error
			pessimizationDone := driverTraceOnPessimization(c.driver.trace, ctx, c.Address(), err)
			pessimizationDone(ctx, c.Address(), c.driver.cluster.Pessimize(c.addr))
		}
		return
	}

	s, err := raw.NewStream(ctx, desc, method, append(opts, grpc.MaxCallRecvMsgSize(50*1024*1024))...)
	if err != nil {
		return nil, mapGRPCError(err)
	}

	return &grpcClientStream{
		ctx:    rawCtx,
		c:      c,
		d:      c.driver,
		s:      s,
		cancel: cancel,
		recv:   streamRecv,
	}, nil
}

func (c *conn) Address() string {
	if c != nil {
		return c.addr.String()
	}
	return ""
}

func newConn(addr connAddr, dial func(context.Context, string, int) (*grpc.ClientConn, error), ttl time.Duration) *conn {
	const (
		statsDuration = time.Minute
		statsBuckets  = 12
	)
	if ttl <= 0 {
		ttl = time.Minute
	}
	c := &conn{
		mtx:   &sync.Mutex{},
		addr:  addr,
		dial:  dial,
		ttl:   ttl,
		timer: timeutil.NewTimer(ttl),
		done:  make(chan struct{}),
		runtime: &connRuntime{
			opTime:  stats.NewSeries(statsDuration, statsBuckets),
			opRate:  stats.NewSeries(statsDuration, statsBuckets),
			errRate: stats.NewSeries(statsDuration, statsBuckets),
		},
	}
	go c.waitClose()
	return c
}
