package ydb

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/yandex-cloud/ydb-go-sdk/api"
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
	DefaultBalancingMethod = BalancingRandomChoice

	// DefaultContextDeadlineMapping contains driver's default behavior of how
	// to use context's deadline value.
	DefaultContextDeadlineMapping = ContextDeadlineOperationTimeout

	// ErrClosed is returned when operation requested on a closed driver.
	ErrClosed = errors.New("driver closed")

	// ErrNilConnection is returned when use nil preferred connection
	ErrNilConnection = errors.New("nil connection")
)

// EndpointInfo is struct contained information about endpoint
type EndpointInfo interface {
	Conn() *conn
	Address() string
}

// CallInfo is struct contained information about call
type CallInfo interface {
	EndpointInfo
}

type callInfo struct {
	conn *conn
}

func (info *callInfo) Conn() *conn {
	return info.conn
}

func (info *callInfo) Address() string {
	return info.conn.addr.String()
}

type ConnUsePolicy uint8

const (
	ConnUseDefault ConnUsePolicy = 1 << iota >> 1
	ConnUseBalancer
	ConnUseEndpoint

	ConnUseSmart = ConnUseBalancer | ConnUseEndpoint
)

// Driver is an interface of YDB driver.
type Driver interface {
	Call(context.Context, api.Operation) (CallInfo, error)
	StreamRead(context.Context, api.StreamOperation) (CallInfo, error)
	Close() error
}

// BalancingMethod encodes balancing method for driver configuration.
type BalancingMethod uint

const (
	BalancingUnknown BalancingMethod = iota
	BalancingRoundRobin
	BalancingP2C
	BalancingRandomChoice
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
	BalancingRandomChoice: func(_ interface{}) balancer {
		return &randomChoice{
			r: rand.New(rand.NewSource(time.Now().UnixNano())),
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
	// regardless of the cancellation appropriate error will be returned to
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

	// RequestsType set an additional type hint to all requests.
	// It is needed only for debug purposes and advanced cases.
	RequestsType string

	// FastDial will make dialer return Driver as soon as 1st connection succeeds.
	// NB: it may be not the fastest node to serve requests.
	FastDial bool
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
	cluster *cluster
	meta    *meta
	trace   DriverTrace

	requestTimeout       time.Duration
	streamTimeout        time.Duration
	operationTimeout     time.Duration
	operationCancelAfter time.Duration

	contextDeadlineMapping ContextDeadlineMapping
}

func (d *driver) Close() error {
	return d.cluster.Close()
}

func (d *driver) Call(ctx context.Context, op api.Operation) (info CallInfo, err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

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
	var md metadata.MD
	md, err = d.meta.md(ctx)
	if err != nil {
		return
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	conn, backoffUseBalancer := ContextConn(rawCtx)
	if backoffUseBalancer && (conn == nil || conn.runtime.getState() != ConnOnline) {
		d.trace.getConnStart(rawCtx)
		conn, err = d.cluster.Get(ctx)
		d.trace.getConnDone(rawCtx, conn, err)
		if err != nil {
			return
		}
	}

	info = &callInfo{
		conn: conn,
	}

	if conn.conn == nil {
		return info, ErrNilConnection
	}

	method, req, res, resp := internal.Unwrap(op)
	if resp == nil {
		resp = internal.WrapOpResponse(&Ydb_Operations.GetOperationResponse{})
	}

	params, ok := operationParams(ctx, d.contextDeadlineMapping)
	if ok {
		setOperationParams(req, params)
	}

	start := timeutil.Now()
	conn.runtime.operationStart(start)
	d.trace.operationStart(rawCtx, conn, method, params)

	err = invoke(ctx, conn.conn, resp, method, req, res)

	conn.runtime.operationDone(
		start, timeutil.Now(),
		errIf(isTimeoutError(err), err),
	)
	d.trace.operationDone(rawCtx, conn, method, params, resp, err)

	if err != nil {
		if te, ok := err.(*TransportError); ok && te.Reason != TransportErrorCanceled {
			// remove node from discovery cache on any transport error
			d.trace.pessimizationStart(rawCtx, &conn.addr, err)
			d.trace.pessimizationDone(rawCtx, &conn.addr, d.cluster.Pessimize(conn.addr))
		}
	}

	return
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

func (d *driver) StreamRead(ctx context.Context, op api.StreamOperation) (info CallInfo, err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

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
		return
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	conn, backoffUseBalancer := ContextConn(rawCtx)
	if backoffUseBalancer && (conn == nil || conn.runtime.getState() != ConnOnline) {
		d.trace.getConnStart(rawCtx)
		conn, err = d.cluster.Get(ctx)
		d.trace.getConnDone(rawCtx, conn, err)
	}

	info = &callInfo{
		conn: conn,
	}

	if err != nil {
		return
	}

	method, req, resp, process := internal.UnwrapStreamOperation(op)
	desc := grpc.StreamDesc{
		StreamName:    path.Base(method),
		ServerStreams: true,
	}

	conn.runtime.streamStart(timeutil.Now())
	d.trace.streamStart(rawCtx, conn, method)
	defer func() {
		if err != nil {
			conn.runtime.streamDone(timeutil.Now(), err)
			d.trace.streamDone(rawCtx, conn, method, err)
		}
	}()

	s, err := grpc.NewClientStream(ctx, &desc, conn.conn, method,
		grpc.MaxCallRecvMsgSize(50*1024*1024), // 50MB
	)
	if err != nil {
		return info, mapGRPCError(err)
	}
	if err := s.SendMsg(req); err != nil {
		return info, mapGRPCError(err)
	}
	if err := s.CloseSend(); err != nil {
		return info, mapGRPCError(err)
	}

	go func() {
		var err error
		defer func() {
			conn.runtime.streamDone(timeutil.Now(), hideEOF(err))
			d.trace.streamDone(rawCtx, conn, method, hideEOF(err))
			if cancel != nil {
				cancel()
			}
		}()
		for err == nil {
			conn.runtime.streamRecv(timeutil.Now())
			d.trace.streamRecvStart(rawCtx, conn, method)

			err = s.RecvMsg(resp)

			d.trace.streamRecvDone(rawCtx, conn, method, resp, hideEOF(err))
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

	return info, nil
}

func invoke(
	ctx context.Context, conn *grpc.ClientConn,
	resp internal.Response,
	method string, req, res proto.Message,
	opts ...grpc.CallOption,
) (
	err error,
) {
	err = conn.Invoke(ctx, method, req, resp.GetResponseProto(), opts...)
	switch {
	case err != nil:
		err = mapGRPCError(err)

	case !resp.GetOpReady():
		err = ErrOperationNotReady

	case resp.GetStatus() != Ydb.StatusIds_SUCCESS:
		err = &OpError{
			Reason: statusCode(resp.GetStatus()),
			issues: resp.GetIssues(),
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
	return proto.Unmarshal(resp.GetResult().Value, res)
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
	mu           sync.RWMutex
	state        ConnState
	offlineCount uint64
	opStarted    uint64
	opSucceed    uint64
	opFailed     uint64
	opTime       *stats.Series
	opRate       *stats.Series
	errRate      *stats.Series
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

type ConnState int8

const (
	ConnOffline ConnState = iota - 2
	ConnBanned
	ConnStateUnknown
	ConnOnline
)

func (s ConnState) String() string {
	switch s {
	case ConnOnline:
		return "online"
	case ConnOffline:
		return "offline"
	case ConnBanned:
		return "banned"
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
	if s == ConnOffline {
		c.offlineCount++
	}
}

func (c *connRuntime) getState() (s ConnState) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
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
