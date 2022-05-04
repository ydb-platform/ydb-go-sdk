package conn

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/response"
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

func (c *conn) Address() string {
	return c.endpoint.Address()
}

type conn struct {
	mtx       sync.RWMutex
	config    Config // ro access
	cc        *grpc.ClientConn
	done      chan struct{}
	endpoint  endpoint.Endpoint // ro access
	closed    bool
	state     State
	lastUsage time.Time
	onClose   []func(*conn)
}

func (c *conn) Ping(ctx context.Context) error {
	cc, err := c.take(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	if !isAvailable(cc) {
		return xerrors.WithStackTrace(errUnavailableConnection)
	}
	return nil
}

func (c *conn) LastUsage() time.Time {
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
		return xerrors.WithStackTrace(err)
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

func (c *conn) Unban() State {
	c.mtx.Lock()
	defer c.mtx.Unlock()

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
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.state
}

func (c *conn) take(ctx context.Context) (cc *grpc.ClientConn, err error) {
	onDone := trace.DriverOnConnTake(
		c.config.Trace(),
		&ctx,
		c.endpoint.Copy(),
	)

	defer func() {
		onDone(err)
	}()

	if c.isClosed() {
		return nil, xerrors.WithStackTrace(errClosedConnection)
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.cc != nil {
		return c.cc, nil
	}

	if dialTimeout := c.config.DialTimeout(); dialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dialTimeout)
		defer cancel()
	}

	// prepend "ydb" scheme for grpc dns-xresolver to find the proper scheme
	// ydb:///", three slashes is ok. It need for good parse scheme in grpc resolver.
	address := "ydb:///" + c.endpoint.Address()

	cc, err = grpc.DialContext(ctx, address, c.config.GrpcDialOptions()...)
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("dial %s failed: %w", address, err))
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
	return xerrors.WithStackTrace(err)
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

	return xerrors.WithStackTrace(err)
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
			c.config.Trace(),
			&ctx,
			c.endpoint,
			trace.Method(method),
		)
		cc *grpc.ClientConn
	)

	defer func() {
		onDone(err, issues, opID, c.GetState())
	}()

	cc, err = c.take(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	c.touchLastUsage()
	defer c.touchLastUsage()

	err = cc.Invoke(ctx, method, req, res, opts...)

	if err != nil {
		if wrapping {
			return xerrors.WithStackTrace(
				xerrors.FromGRPCError(
					err,
					xerrors.WithAddress(c.Address()),
				),
			)
		}
		return xerrors.WithStackTrace(err)
	}

	if o, ok := res.(response.Response); ok {
		opID = o.GetOperation().GetId()
		for _, issue := range o.GetOperation().GetIssues() {
			issues = append(issues, issue)
		}
		if wrapping {
			switch {
			case !o.GetOperation().GetReady():
				return xerrors.WithStackTrace(errOperationNotReady)

			case o.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
				return xerrors.WithStackTrace(
					xerrors.Operation(
						xerrors.FromOperation(
							o.GetOperation(),
						),
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
		wrapping = needWrapping(ctx)
		cc       *grpc.ClientConn
		s        grpc.ClientStream
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

	cc, err = c.take(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	c.touchLastUsage()
	defer c.touchLastUsage()

	s, err = cc.NewStream(ctx, desc, method, opts...)

	if err != nil {
		if wrapping {
			return s, xerrors.WithStackTrace(
				xerrors.FromGRPCError(
					err,
					xerrors.WithAddress(c.Address()),
				),
			)
		}
		return s, xerrors.WithStackTrace(err)
	}

	return &grpcClientStream{
		ClientStream: s,
		c:            c,
		wrapping:     wrapping,
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
