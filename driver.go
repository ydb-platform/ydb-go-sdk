package ydb

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

	// DefaultContextDeadlineMapping contains driver's default behavior of how
	// to use context's deadline value.
	DefaultContextDeadlineMapping = ContextDeadlineOperationTimeout
)

// ErrClosed is returned when operation requested on a closed driver.
var ErrClosed = errors.New("driver closed")

// Driver is an interface of YDB driver.
type Driver interface {
	Call(context.Context, internal.Operation) error
	StreamRead(context.Context, internal.StreamOperation) error
	Close() error
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

	// OperationTimeout is the maximum amount of time a YDB server will process
	// an operation. After timeout exceeds YDB will try to cancel operation and
	// regardless of the cancelation appropriate error will be returned to
	// the client.
	// If OperationTimeout is zero then no timeout is used.
	OperationTimeout time.Duration

	// OperationCancelAfter is the maximum amount of time a YDB server will process an
	// operation. After timeout exceeds YDB will try to cancel operation and if
	// it succeeds appropriate error will be returned to the client; otherwise
	// processing will be continued.
	// If OperationCancelAfter is zero then no timeout is used.
	OperationCancelAfter time.Duration

	// ContextDeadlineMapping describes how context.Context's deadline value is
	// used for YDB operation options. That is, when neither OperationTimeout
	// nor OperationCancelAfter defined as context's values or driver options.
	//
	// If ContextDeadlineMapping is zero then the DefaultContextDeadlineMapping
	// value is used.
	ContextDeadlineMapping ContextDeadlineMapping

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
	if c.ContextDeadlineMapping == 0 {
		c.ContextDeadlineMapping = DefaultContextDeadlineMapping
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

	// TLSConfig specifies the TLS configuration to use for tls client.
	// If TLSConfig is zero then connections are insecure.
	TLSConfig *tls.Config

	// Timeout is the maximum amount of time a dial will wait for a connect to
	// complete.
	// If Timeout is zero then no timeout is used.
	Timeout time.Duration
}

// Dial dials given addr and initializes driver instance on success.
func (d *Dialer) Dial(ctx context.Context, addr string) (Driver, error) {
	config := d.DriverConfig.withDefaults()
	return (&dialer{
		netDial:   d.NetDial,
		tlsConfig: d.TLSConfig,
		timeout:   d.Timeout,
		config:    config,
		meta: &meta{
			trace:       config.Trace,
			database:    config.Database,
			credentials: config.Credentials,
		},
	}).dial(ctx, addr)
}

// dialer is an instance holding single Dialer.Dial() configuration parameters.
type dialer struct {
	netDial   func(context.Context, string) (net.Conn, error)
	tlsConfig *tls.Config
	timeout   time.Duration
	config    DriverConfig
	meta      *meta
}

func (d *dialer) dial(ctx context.Context, addr string) (Driver, error) {
	cluster := cluster{
		dial: d.dialHostPort,
	}
	var explorer *repeater
	if d.config.DiscoveryInterval > 0 {
		cluster.balancer = balancers[d.config.BalancingMethod]()

		curr, err := d.discover(ctx, addr)
		if err != nil {
			return nil, err
		}
		// Sort current list of endpoints to prevent additional sorting withing
		// background discovery below.
		sortEndpoints(curr)
		for _, e := range curr {
			cluster.Insert(ctx, e)
		}
		explorer = &repeater{
			Interval: d.config.DiscoveryInterval,
			Task: func(ctx context.Context) {
				next, err := d.discover(ctx, addr)
				if err != nil {
					return
				}
				// NOTE: curr endpoints must be sorted here.
				sortEndpoints(next)
				diffEndpoints(curr, next,
					func(i, j int) {
						// Endpoints are equal but we still need to update meta
						// data such that load factor and others.
						cluster.Update(ctx, next[j])
					},
					func(i, j int) {
						cluster.Insert(ctx, next[j])
					},
					func(i, j int) {
						cluster.Remove(ctx, curr[i])
					},
				)
				curr = next
			},
		}
		explorer.Start()
	} else {
		var (
			e   Endpoint
			err error
		)
		e.Addr, e.Port, err = splitHostPort(addr)
		if err != nil {
			return nil, err
		}

		cluster.balancer = new(singleConnBalancer)
		cluster.Insert(ctx, e)

		// Ensure that endpoint is connectable.
		_, err = cluster.Get(ctx)
		if err != nil {
			return nil, err
		}
	}
	return &driver{
		cluster:                &cluster,
		explorer:               explorer,
		meta:                   d.meta,
		trace:                  d.config.Trace,
		requestTimeout:         d.config.RequestTimeout,
		streamTimeout:          d.config.StreamTimeout,
		operationTimeout:       d.config.OperationTimeout,
		operationCancelAfter:   d.config.OperationCancelAfter,
		contextDeadlineMapping: d.config.ContextDeadlineMapping,
	}, nil
}

func (d *dialer) dialHostPort(ctx context.Context, host string, port int) (*conn, error) {
	rawctx := ctx
	if d.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}
	addr := connAddr{
		addr: host,
		port: port,
	}
	s := addr.String()
	d.config.Trace.dialStart(rawctx, s)

	cc, err := grpc.DialContext(ctx, s, d.grpcDialOptions()...)

	d.config.Trace.dialDone(rawctx, s, err)
	if err != nil {
		return nil, err
	}

	return newConn(cc, addr), nil
}

func (d *dialer) dialAddr(ctx context.Context, addr string) (*conn, error) {
	host, port, err := splitHostPort(addr)
	if err != nil {
		return nil, err
	}
	return d.dialHostPort(ctx, host, port)
}

func (d *dialer) discover(ctx context.Context, addr string) (endpoints []Endpoint, err error) {
	conn, err := d.dialAddr(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.conn.Close()

	d.config.Trace.discoveryStart(ctx)
	defer func() {
		d.config.Trace.discoveryDone(ctx, endpoints, err)
	}()

	subctx := ctx
	if d.timeout > 0 {
		var cancel context.CancelFunc
		subctx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}

	return (&discoveryClient{
		conn: conn,
		meta: d.meta,
	}).Discover(subctx, d.config.Database)
}

func (d *dialer) grpcDialOptions() (opts []grpc.DialOption) {
	if d.netDial != nil {
		opts = append(opts, grpc.WithDialer(withContextDialer(d.netDial)))
	}
	if c := d.tlsConfig; c != nil {
		opts = append(opts, grpc.WithTransportCredentials(
			credentials.NewTLS(c),
		))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	return append(opts,
		grpc.WithBlock(),
	)
}

type driver struct {
	cluster  *cluster
	meta     *meta
	trace    DriverTrace
	explorer *repeater

	requestTimeout       time.Duration
	streamTimeout        time.Duration
	operationTimeout     time.Duration
	operationCancelAfter time.Duration

	contextDeadlineMapping ContextDeadlineMapping
}

func (d *driver) Close() error {
	if d.explorer != nil {
		d.explorer.Stop()
	}
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
	if t := d.operationTimeout; t > 0 {
		ctx = WithOperationTimeout(ctx, t)
	}
	if t := d.operationCancelAfter; t > 0 {
		ctx = WithOperationCancelAfter(ctx, t)
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
	d.trace.getConnDone(rawctx, conn, err)
	if err != nil {
		return err
	}

	var resp Ydb_Operations.GetOperationResponse
	method, req, res := internal.Unwrap(op)

	params, ok := operationParams(ctx, d.contextDeadlineMapping)
	if ok {
		setOperationParams(req, params)
	}

	start := timeutil.Now()
	conn.runtime.operationStart(start)
	d.trace.operationStart(rawctx, conn, method, params)

	err = invoke(ctx, conn.conn, &resp, method, req, res)

	conn.runtime.operationDone(
		start, timeutil.Now(),
		errIf(isTimeoutError(err), err),
	)
	d.trace.operationDone(rawctx, conn, method, params, resp, err)

	return err
}

func isTimeoutError(err error) bool {
	return IsOpError(err, StatusTimeout) ||
		IsOpError(err, StatusCancelled) ||
		IsBusyAfter(err)
}

func IsBusyAfter(err error) bool {
	if _, ok := err.(*TransportError); ok {
		return true
	}
	if err == context.DeadlineExceeded {
		return true
	}
	if err == context.Canceled {
		return true
	}
	return false
}

func errIf(cond bool, err error) error {
	if cond {
		return err
	}
	return nil
}

func (d *driver) StreamRead(ctx context.Context, op internal.StreamOperation) (err error) {
	// Remember raw context to pass it for the tracing functions.
	rawctx := ctx

	var cancel context.CancelFunc
	if t := d.streamTimeout; t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
		defer func() {
			if err != nil {
				cancel()
			}
		}()
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
	d.trace.getConnDone(rawctx, conn, err)
	if err != nil {
		return err
	}

	method, req, resp, process := internal.UnwrapStreamOperation(op)
	desc := grpc.StreamDesc{
		StreamName:    path.Base(method),
		ServerStreams: true,
	}

	conn.runtime.streamStart(timeutil.Now())
	d.trace.streamStart(rawctx, conn, method)
	defer func() {
		if err != nil {
			conn.runtime.streamDone(timeutil.Now(), err)
			d.trace.streamDone(rawctx, conn, method, err)
		}
	}()

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
		var err error
		defer func() {
			conn.runtime.streamDone(timeutil.Now(), hideEOF(err))
			d.trace.streamDone(rawctx, conn, method, hideEOF(err))
			if cancel != nil {
				cancel()
			}
		}()
		for err == nil {
			conn.runtime.streamRecv(timeutil.Now())
			d.trace.streamRecvStart(rawctx, conn, method)

			err = s.RecvMsg(resp)

			d.trace.streamRecvDone(rawctx, conn, method, resp, hideEOF(err))
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
			// NOTE: do not hide even io.EOF for this call.
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

func (c *connRuntime) streamStart(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opRate.Add(now, 1)
}

func (c *connRuntime) streamRecv(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opRate.Add(now, 1)
}

func (c *connRuntime) streamDone(now time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.errRate.Add(now, 1)
	}
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

func splitHostPort(addr string) (host string, port int, err error) {
	var prt string
	host, prt, err = net.SplitHostPort(addr)
	if err != nil {
		return
	}
	port, err = strconv.Atoi(prt)
	return
}

func hideEOF(err error) error {
	if err == io.EOF {
		return nil
	}
	return err
}
