package conn

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/response"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// nolint:gofumpt
// nolint:nolintlint
var (
	// ErrOperationNotReady specified error when operation is not ready
	ErrOperationNotReady = fmt.Errorf("operation is not ready yet")
)

type Conn interface {
	grpc.ClientConnInterface

	Endpoint() endpoint.Endpoint

	LastUsage() time.Time

	IsState(states ...State) bool
	GetState() State
	SetState(State) State

	Release(ctx context.Context)
}

func (c *conn) Address() string {
	return c.endpoint.Address()
}

type conn struct {
	mtx          sync.RWMutex
	config       Config // ro access
	cc           *grpc.ClientConn
	done         chan struct{}
	endpoint     endpoint.Endpoint // ro access
	closed       bool
	state        State
	usages       int32
	streamUsages int32
	lastUsage    time.Time
	onClose      []func(*conn)
}

func (c *conn) Release(ctx context.Context) {
	var (
		onDone = trace.DriverOnConnRelease(
			c.config.Trace(),
			&ctx,
			c.endpoint.Copy(),
		)
		err error
	)
	defer func() {
		onDone(err)
	}()
	var issues []error
	if c.changeUsages(-1) == 0 {
		if usages := atomic.LoadInt32(&c.streamUsages); usages > 0 {
			issues = append(issues, fmt.Errorf("conn in stream use: usages=%d", usages))
		}
		if closeErr := c.Close(ctx); closeErr != nil {
			issues = append(issues, closeErr)
		}
	}
	if len(issues) > 0 {
		err = errors.NewWithIssues("conn released with issues", issues...)
	}
}

func (c *conn) LastUsage() time.Time {
	if usages := atomic.LoadInt32(&c.streamUsages); usages > 0 {
		return time.Now()
	}
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.lastUsage
}

func (c *conn) IsState(states ...State) bool {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	for _, s := range states {
		if s == c.state {
			return true
		}
	}

	return false
}

func (c *conn) park(ctx context.Context) (err error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.closed {
		return nil
	}

	if c.cc == nil {
		return nil
	}

	onDone := trace.DriverOnConnPark(
		c.config.Trace(),
		&ctx,
		c.Endpoint(),
	)
	defer func() {
		onDone(err)
	}()

	err = c.close()

	if err != nil {
		return errors.WithStackTrace(err)
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
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.setState(s)
}

func (c *conn) setState(s State) State {
	trace.DriverOnConnStateChange(
		c.config.Trace(),
		c.endpoint.Copy(),
		c.state,
	)(s)
	c.state = s
	return c.state
}

func (c *conn) GetState() (s State) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.state
}

func (c *conn) take(ctx context.Context) (cc *grpc.ClientConn, err error) {
	onDone := trace.DriverOnConnTake(
		trace.ContextDriver(ctx).Compose(c.config.Trace()),
		&ctx,
		c.endpoint.Copy(),
	)

	defer func() {
		onDone(err)
	}()

	if c.isClosed() {
		return nil, errors.NewGrpcError(
			codes.Unavailable,
			errors.WithMsg("ydb driver conn closed early"),
		)
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if !isBroken(c.cc) {
		return c.cc, nil
	}

	_ = c.close()

	if dialTimeout := c.config.DialTimeout(); dialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dialTimeout)
		defer cancel()
	}

	cc, err = grpc.DialContext(ctx, "ydb:///"+c.endpoint.Address(), c.config.GrpcDialOptions()...)
	if err != nil {
		return nil, errors.WithStackTrace(err)
	}

	c.cc = cc
	c.setState(Online)

	return c.cc, nil
}

func (c *conn) touchLastUsage() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.lastUsage = time.Now()
}

func (c *conn) changeUsages(delta int32) int32 {
	defer c.touchLastUsage()

	usages := atomic.AddInt32(&c.usages, delta)

	if usages < 0 {
		panic("negative usages" + strconv.Itoa(int(usages)))
	}

	trace.DriverOnConnUsagesChange(
		c.config.Trace(),
		c.endpoint.Copy(),
		int(usages),
	)

	return usages
}

func (c *conn) changeStreamUsages(delta int32) {
	defer c.touchLastUsage()

	usages := atomic.AddInt32(&c.streamUsages, delta)

	if usages < 0 {
		panic("negative stream usages" + strconv.Itoa(int(usages)))
	}

	trace.DriverOnConnStreamUsagesChange(
		c.config.Trace(),
		c.endpoint.Copy(),
		int(usages),
	)
}

func isBroken(raw *grpc.ClientConn) bool {
	if raw == nil {
		return true
	}
	s := raw.GetState()
	return s == connectivity.Shutdown || s == connectivity.TransientFailure
}

// conn must be locked
func (c *conn) close() (err error) {
	if c.cc == nil {
		return nil
	}
	err = c.cc.Close()
	c.cc = nil
	c.setState(Offline)
	return err
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

	for _, f := range c.onClose {
		f(c)
	}

	return err
}

func (c *conn) invoke(
	ctx context.Context,
	method string,
	req interface{},
	res interface{},
	opts ...grpc.CallOption,
) (err error) {
	var cc *grpc.ClientConn
	cc, err = c.take(ctx)
	if err != nil {
		return errors.NewGrpcError(
			codes.Unavailable,
			errors.WithMsg("ydb driver conn take failed"),
			errors.WithErr(err),
		)
	}

	c.changeUsages(1)
	defer c.changeUsages(-1)

	return cc.Invoke(ctx, method, req, res, opts...)
}

func (c *conn) Invoke(
	ctx context.Context,
	method string,
	req interface{},
	res interface{},
	opts ...grpc.CallOption,
) (err error) {
	var (
		opID     string
		issues   []trace.Issue
		wrapping = needWrapping(ctx)
		onDone   = trace.DriverOnConnInvoke(
			trace.ContextDriver(ctx).Compose(c.config.Trace()),
			&ctx,
			c.endpoint,
			trace.Method(method),
		)
	)

	defer func() {
		onDone(err, issues, opID, c.GetState())
	}()

	err = c.invoke(ctx, method, req, res, opts...)

	if err != nil {
		if wrapping {
			return errors.WithStackTrace(
				errors.MapGRPCError(
					err,
					errors.WithTEAddress(c.Address()),
				),
			)
		}
		return err
	}

	if o, ok := res.(response.Response); ok {
		opID = o.GetOperation().GetId()
		for _, issue := range o.GetOperation().GetIssues() {
			issues = append(issues, issue)
		}
		if wrapping {
			switch {
			case !o.GetOperation().GetReady():
				return errors.WithStackTrace(ErrOperationNotReady)

			case o.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
				return errors.WithStackTrace(errors.NewOpError(errors.WithOEOperation(o.GetOperation())))
			}
		}
	}

	return err
}

func (c *conn) newStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (_ grpc.ClientStream, err error) {
	var cc *grpc.ClientConn
	cc, err = c.take(ctx)
	if err != nil {
		return nil, errors.NewGrpcError(
			codes.Unavailable,
			errors.WithMsg("ydb driver conn take failed"),
			errors.WithErr(err),
		)
	}

	c.changeStreamUsages(1)
	defer c.changeStreamUsages(-1)

	return cc.NewStream(ctx, desc, method, opts...)
}

func (c *conn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (_ grpc.ClientStream, err error) {
	var (
		streamRecv = trace.DriverOnConnNewStream(
			trace.ContextDriver(ctx).Compose(c.config.Trace()),
			&ctx,
			c.endpoint.Copy(),
			trace.Method(method),
		)
		wrapping = needWrapping(ctx)
	)

	defer func() {
		if err != nil {
			streamRecv(err)(c.GetState(), err)
		}
	}()

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)

	defer func() {
		if err != nil {
			cancel()
		}
	}()

	var s grpc.ClientStream
	s, err = c.newStream(
		ctx,
		desc,
		method,
		opts...,
	)

	if err != nil {
		if wrapping {
			return s, errors.WithStackTrace(
				errors.MapGRPCError(
					err,
					errors.WithTEAddress(c.Address()),
				),
			)
		}
		return s, err
	}

	return &grpcClientStream{
		c:        c,
		s:        s,
		wrapping: wrapping,
		onDone: func(ctx context.Context) {
			cancel()
		},
		recv: streamRecv,
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

func newConn(e endpoint.Endpoint, config Config, opts ...option) *conn {
	c := &conn{
		usages:   1,
		state:    Created,
		endpoint: e,
		config:   config,
		done:     make(chan struct{}),
		onClose:  make([]func(*conn), 0),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func New(e endpoint.Endpoint, config Config, opts ...option) Conn {
	return newConn(e, config, opts...)
}
