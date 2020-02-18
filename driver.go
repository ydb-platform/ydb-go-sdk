package ydb

import (
	"context"
	"errors"
	"io"
	"net"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_Operations"
	"github.com/yandex-cloud/ydb-go-sdk/internal"
	"github.com/yandex-cloud/ydb-go-sdk/internal/stats"
	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

var (
	// DefaultDiscoveryInterval contains default duration between discovery
	// requests made by driver.
	DefaultDiscoveryInterval = time.Minute

	// DefaultBalancingMethod contains driver's default balancing algorithm.
	DefaultBalancingMethod = BalancingP2C

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

var balancers = map[BalancingMethod]func(interface{}) balancer{
	BalancingRoundRobin: func(_ interface{}) balancer {
		return new(roundRobin)
	},
	BalancingP2C: func(c interface{}) balancer {
		if c == nil {
			return new(p2c)
		}
		config := c.(*P2CConfig)
		return &p2c{
			Criterion: connRuntimeCriterion{
				PreferLocal:     config.PreferLocal,
				OpTimeThreshold: config.OpTimeThreshold,
			},
		}
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

	// BalancingConfig is an optional configuration related to selected
	// BalancingMethod. That is, some balancing methods allow to be configured.
	BalancingConfig interface{}

	// PreferLocalEndpoints adds endpoint selection logic when local endpoints
	// are always used first.
	// When no alive local endpoints left other endpoints will be used.
	//
	// NOTE: some balancing methods (such as p2c) also may use knowledge of
	// endpoint's locality. Difference is that with PreferLocalEndpoints local
	// endpoints selected separately from others. That is, if there at least
	// one local endpoint it will be used regardless of its performance
	// indicators.
	//
	// NOTE: currently driver (and even ydb itself) does not track load factor
	// of each endpoint properly. Enabling this option may lead to the
	// situation, when all but one nodes in local datacenter become inactive
	// and all clients will overload this single instance very quickly. That
	// is, currently this option may be called as experimental.
	// You have been warned.
	PreferLocalEndpoints bool
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
	return d.cluster.Close()
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
	var te *TransportError

	switch {
	case
		IsOpError(err, StatusTimeout),
		IsOpError(err, StatusCancelled),
		errors.As(err, &te),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
		return true
	default:
		return false
	}
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

func mapGRPCError(err error) error {
	s, ok := status.FromError(err)
	if !ok {
		return err
	}
	return &TransportError{
		Reason:  transportErrorCode(s.Code()),
		message: s.Message(),
		err:     err,
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
	state     ConnState
	opStarted uint64
	opSucceed uint64
	opFailed  uint64
	opTime    *stats.Series
	opRate    *stats.Series
	errRate   *stats.Series
}

type ConnStats struct {
	State        ConnState
	OpStarted    uint64
	OpFailed     uint64
	OpSucceed    uint64
	OpPerMinute  float64
	ErrPerMinute float64
	AvgOpTime    time.Duration
}

type ConnState uint

const (
	ConnStateUnknown ConnState = iota
	ConnOnline
	ConnOffline
)

func (s ConnState) String() string {
	switch s {
	case ConnOnline:
		return "online"
	case ConnOffline:
		return "offline"
	default:
		return "unknown"
	}
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
		State:        c.state,
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

func (c *connRuntime) setState(s ConnState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = s
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
