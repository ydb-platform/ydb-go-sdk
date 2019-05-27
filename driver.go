package ydb

import (
	"context"
	"errors"
	"net"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/yandex-cloud/ydb-go-sdk/internal"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb_Operations"
	"github.com/yandex-cloud/ydb-go-sdk/internal/stats"
	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

var (
	// DefaultDiscoveryInterval contains default duration between discovery
	// requests made by driver.
	DefaultDiscoveryInterval = time.Minute

	// DefaultBalancingMethod contains driver's default balancing algorithm.
	DefaultBalancingMethod = BalancingRoundRobin
)

// ErrClosed is returned when operation requested on a closed driver.
var ErrClosed = errors.New("driver closed")

// Driver is an interface of YDB driver.
type Driver interface {
	Call(context.Context, internal.Operation) error
	StreamRead(context.Context, internal.StreamOperation) error
	Close() error
}

// Credentials is an interface that contains options used to authorize a
// client.
type Credentials interface {
	Token(context.Context) (string, error)
}

// AuthTokenCredentials implements Credentials interface with static
// authorization parameters.
type AuthTokenCredentials struct {
	AuthToken string
}

// Token implements Credentials.
func (a AuthTokenCredentials) Token(_ context.Context) (string, error) {
	return a.AuthToken, nil
}

// BalancingMethod encodes balancing method for driver configuration.
type BalancingMethod uint

const (
	BalancingUnknown BalancingMethod = iota
	BalancingRoundRobin
	BalancingP2C
)

var balancers = map[BalancingMethod]func() balancer{
	BalancingRoundRobin: func() balancer {
		return new(roundRobin)
	},
	BalancingP2C: func() balancer {
		return new(p2c)
	},
}

// DriverConfig contains driver configuration options.
type DriverConfig struct {
	// Database is a required database name.
	Database string

	// Credentials is an ydb client credentials.
	// In most cases Credentials are required.
	Credentials Credentials

	// Trace contains driver tracing options.
	Trace DriverTrace

	// RequestTimeout is the maximum amount of time a Call() will wait for an
	// operation to complete.
	// If RequestTimeout is zero then no timeout is used.
	RequestTimeout time.Duration

	// StreamTimeout is the maximum amount of time a StreamRead() will wait for
	// an operation to complete.
	// If StreamTimeout is zero then no timeout is used.
	StreamTimeout time.Duration

	// DiscoveryInterval is the frequency of background tasks of ydb endpoints
	// discovery.
	// If DiscoveryInterval is zero then the DefaultDiscoveryInterval is used.
	// If DiscoveryInterval is negative, then no background discovery prepared.
	DiscoveryInterval time.Duration

	// BalancingMethod is an algorithm used by the driver for endpoint
	// selection.
	// If BalancingMethod is zero then the DefaultBalancingMethod is used.
	BalancingMethod BalancingMethod
}

func (d *DriverConfig) withDefaults() (c DriverConfig) {
	if d != nil {
		c = *d
	}
	if c.DiscoveryInterval == 0 {
		c.DiscoveryInterval = DefaultDiscoveryInterval
	}
	if c.BalancingMethod == 0 {
		c.BalancingMethod = DefaultBalancingMethod
	}
	return c
}

// Dialer contains options of dialing and initialization of particular ydb
// driver.
type Dialer struct {
	// DriverConfig is a driver configuration.
	DriverConfig *DriverConfig

	// NetDial is a optional function that may replace default network dialing
	// function such as net.Dial("tcp").
	NetDial func(context.Context, string) (net.Conn, error)

	// Timeout is the maximum amount of time a dial will wait for a connect to
	// complete.
	// If Timeout is zero then no timeout is used.
	Timeout time.Duration
}

// Dial dials to a given addr and initializes driver instance on success.
func (d *Dialer) Dial(ctx context.Context, addr string) (Driver, error) {
	var (
		config = d.DriverConfig.withDefaults()
		trace  = config.Trace
	)
	dial := func(ctx context.Context, host string, port int) (*conn, error) {
		rawctx := ctx
		if d.Timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d.Timeout)
			defer cancel()
		}
		addr := connAddr{
			addr: host,
			port: port,
		}
		s := addr.String()
		trace.dialStart(rawctx, s)

		cc, err := grpc.DialContext(ctx, s, d.grpcDialOptions()...)

		trace.dialDone(rawctx, s, err)
		if err != nil {
			return nil, err
		}
		return newConn(cc, addr), nil
	}

	host, prt, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(prt)
	if err != nil {
		return nil, err
	}
	econn, err := dial(ctx, host, port)
	if err != nil {
		return nil, err
	}

	c := &cluster{
		dial:     dial,
		balancer: balancers[config.BalancingMethod](),
	}
	m := newMeta(config.Database, config.Credentials)

	discover := func(ctx context.Context) (endpoints []Endpoint, err error) {
		trace.discoveryStart(ctx)
		defer func() {
			trace.discoveryDone(ctx, endpoints, err)
		}()

		ds := discoveryClient{
			conn: econn,
			meta: m,
		}
		subctx := ctx
		if d.Timeout > 0 {
			var cancel context.CancelFunc
			subctx, cancel = context.WithTimeout(ctx, d.Timeout)
			defer cancel()
		}
		return ds.Discover(subctx, config.Database)
	}

	curr, err := discover(ctx)
	if err != nil {
		return nil, err
	}
	// Sort current list of endpoints to prevent additional sorting withing
	// background discovery below.
	sortEndpoints(curr)
	for _, e := range curr {
		c.Insert(ctx, e)
	}

	driver := &driver{
		cluster:        c,
		meta:           m,
		trace:          trace,
		requestTimeout: config.RequestTimeout,
		streamTimeout:  config.StreamTimeout,
	}
	if config.DiscoveryInterval > 0 {
		driver.explorer = repeater{
			Interval: config.DiscoveryInterval,
			Task: func(ctx context.Context) {
				next, err := discover(ctx)
				if err != nil {
					return
				}
				// NOTE: curr endpoints must be sorted here.
				sortEndpoints(next)
				diffEndpoints(curr, next,
					func(i, j int) {
						// Endpoints are equal but we still need to update meta
						// data such that load factor and others.
						c.Update(ctx, next[j])
					},
					func(i, j int) {
						c.Insert(ctx, next[j])
					},
					func(i, j int) {
						c.Remove(ctx, curr[i])
					},
				)
				curr = next
			},
		}
		driver.explorer.Start()
	}

	return driver, nil
}

func (d *Dialer) grpcDialOptions() (opts []grpc.DialOption) {
	if d.NetDial != nil {
		opts = append(opts, grpc.WithDialer(withContextDialer(d.NetDial)))
	}
	return append(opts,
		// TODO: allow secure driver use.
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
}

type driver struct {
	cluster  *cluster
	meta     *meta
	trace    DriverTrace
	explorer repeater

	requestTimeout time.Duration
	streamTimeout  time.Duration
}

func (d *driver) Close() error {
	d.explorer.Stop()
	d.cluster.Close()
	return nil
}

func (d *driver) Call(ctx context.Context, op internal.Operation) error {
	// Remember raw context to pass it for the tracing functions.
	rawctx := ctx

	if t := d.requestTimeout; t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}

	// Get credentials (token actually) for the request.
	md, err := d.meta.md(ctx)
	if err != nil {
		return err
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	d.trace.getConnStart(rawctx)
	conn, err := d.cluster.Get(ctx)
	d.trace.getConnDone(rawctx, conn.addr.String(), err)
	if err != nil {
		return err
	}

	var resp Ydb_Operations.GetOperationResponse
	method, req, res := internal.Unwrap(op)

	start := timeutil.Now()
	conn.runtime.operationStart(start)
	d.trace.operationStart(rawctx, conn, method)

	err = invoke(ctx, conn.conn, &resp, method, req, res)

	conn.runtime.operationDone(start, timeutil.Now(), transportErrorOrNil(err))
	d.trace.operationDone(rawctx, conn, method, resp, err)

	return err
}

func transportErrorOrNil(err error) error {
	if err == context.DeadlineExceeded {
		// Note that we do not use here context.Canceled error due to
		// cancelation may occur by custom application logic and does not
		// relate to quality of connection.
		return err
	}
	if _, ok := err.(*TransportError); ok {
		return err
	}
	return nil
}

func (d *driver) StreamRead(ctx context.Context, op internal.StreamOperation) error {
	// Remember raw context to pass it for the tracing functions.
	rawctx := ctx

	if t := d.streamTimeout; t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}

	// Get credentials (token actually) for the request.
	md, err := d.meta.md(ctx)
	if err != nil {
		return err
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	d.trace.getConnStart(rawctx)
	conn, err := d.cluster.Get(ctx)
	d.trace.getConnDone(rawctx, conn.addr.String(), err)
	if err != nil {
		return err
	}

	method, req, resp, process := internal.UnwrapStreamOperation(op)
	desc := grpc.StreamDesc{
		StreamName:    path.Base(method),
		ServerStreams: true,
	}
	s, err := grpc.NewClientStream(ctx, &desc, conn.conn, method,
		grpc.MaxCallRecvMsgSize(50*1024*1024), // 50MB
	)
	if err != nil {
		return mapGRPCError(err)
	}
	if err := s.SendMsg(req); err != nil {
		return mapGRPCError(err)
	}
	if err := s.CloseSend(); err != nil {
		return mapGRPCError(err)
	}

	go func() {
		for err := (error)(nil); err == nil; {
			err = s.RecvMsg(resp)
			if err != nil {
				err = mapGRPCError(err)
			} else {
				if s := resp.GetStatus(); s != Ydb.StatusIds_SUCCESS {
					err = &OpError{
						Reason: statusCode(s),
						issues: resp.GetIssues(),
					}
				}
			}
			process(err)
		}
	}()

	return nil
}

func invoke(
	ctx context.Context, conn *grpc.ClientConn,
	resp *Ydb_Operations.GetOperationResponse,
	method string, req, res proto.Message,
	opts ...grpc.CallOption,
) (
	err error,
) {
	err = grpc.Invoke(ctx, method, req, resp, conn, opts...)
	op := resp.Operation
	switch {
	case err != nil:
		err = mapGRPCError(err)

	case !op.Ready:
		err = ErrOperationNotReady

	case op.Status != Ydb.StatusIds_SUCCESS:
		err = &OpError{
			Reason: statusCode(op.Status),
			issues: op.Issues,
		}
	}
	if err != nil {
		return err
	}
	if res == nil {
		// NOTE: YDB API at this moment supports extension of its protocol by
		// adding Result structures. That is, one may think that no result is
		// provided by some call, but some day it may change and client
		// implementation will lag some time – no strict behavior is possible.
		return nil
	}
	return proto.Unmarshal(op.Result.Value, res)
}

func Dial(ctx context.Context, addr string, c *DriverConfig) (Driver, error) {
	d := Dialer{
		DriverConfig: c,
	}
	return d.Dial(ctx, addr)
}

func mapGRPCError(err error) error {
	grpcErr, ok := err.(interface {
		GRPCStatus() *status.Status
	})
	if !ok {
		return err
	}
	s := grpcErr.GRPCStatus()
	return &TransportError{
		Reason:  transportErrorCode(s.Code()),
		message: s.Message(),
	}
}

type connAddr struct {
	addr string
	port int
}

func (c connAddr) String() string {
	return net.JoinHostPort(c.addr, strconv.Itoa(c.port))
}

type conn struct {
	conn *grpc.ClientConn
	addr connAddr

	runtime connRuntime
}

func newConn(cc *grpc.ClientConn, addr connAddr) *conn {
	const (
		statsDuration = time.Minute
		statsBuckets  = 12
	)
	return &conn{
		conn: cc,
		addr: addr,
		runtime: connRuntime{
			opTime:  stats.NewSeries(statsDuration, statsBuckets),
			opRate:  stats.NewSeries(statsDuration, statsBuckets),
			errRate: stats.NewSeries(statsDuration, statsBuckets),
		},
	}
}

type connRuntime struct {
	mu        sync.Mutex
	opStarted uint64
	opSucceed uint64
	opFailed  uint64
	opTime    *stats.Series
	opRate    *stats.Series
	errRate   *stats.Series
}

type ConnStats struct {
	OpStarted    uint64
	OpFailed     uint64
	OpSucceed    uint64
	OpPerMinute  float64
	ErrPerMinute float64
	AvgOpTime    time.Duration
}

func ReadConnStats(d Driver, f func(Endpoint, ConnStats)) {
	x, ok := d.(*driver)
	if !ok {
		return
	}
	x.cluster.Stats(f)
}

func (c ConnStats) OpPending() uint64 {
	return c.OpStarted - (c.OpFailed + c.OpSucceed)
}

func (c *connRuntime) stats() ConnStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := timeutil.Now()

	r := ConnStats{
		OpStarted:    c.opStarted,
		OpSucceed:    c.opSucceed,
		OpFailed:     c.opFailed,
		OpPerMinute:  c.opRate.SumPer(now, time.Minute),
		ErrPerMinute: c.errRate.SumPer(now, time.Minute),
	}
	if rtSum, rtCnt := c.opTime.Get(now); rtCnt > 0 {
		r.AvgOpTime = time.Duration(rtSum / float64(rtCnt))
	}

	return r
}

func (c *connRuntime) operationStart(start time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.opStarted++
	c.opRate.Add(start, 1)
}

func (c *connRuntime) operationDone(start, end time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err != nil {
		c.opFailed++
		c.errRate.Add(end, 1)
	} else {
		c.opSucceed++
	}
	c.opTime.Add(end, float64(end.Sub(start)))
}

// withContextDialer is an adapter to allow the use of normal go-world net dial
// function as WithDialer option argument for grpc Dial().
func withContextDialer(f func(context.Context, string) (net.Conn, error)) func(string, time.Duration) (net.Conn, error) {
	if f == nil {
		return nil
	}
	return func(addr string, timeout time.Duration) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return f(ctx, addr)
	}
}
