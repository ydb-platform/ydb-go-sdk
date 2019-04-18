package ydb

import (
	"context"
	"errors"
	"net"
	"path"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/yandex-cloud/ydb-go-sdk/internal"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb_Operations"
	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

const (
	DefaultDiscoveryInterval = time.Minute
)

var ErrClosed = errors.New("driver closed")

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

type DiscoveryConfig struct {
	Interval time.Duration
	Timer    timeutil.Timer
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
}

func (d *DriverConfig) withDefaults() (c DriverConfig) {
	if d != nil {
		c = *d
	}
	if c.DiscoveryInterval == 0 {
		c.DiscoveryInterval = DefaultDiscoveryInterval
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
		addr := connAddr{
			addr: host,
			port: port,
		}
		s := addr.String()

		trace.dialStart(ctx, s)

		subctx := ctx
		if d.Timeout > 0 {
			var cancel context.CancelFunc
			subctx, cancel = context.WithTimeout(ctx, d.Timeout)
			defer cancel()
		}
		gc, err := grpc.DialContext(subctx, s, d.grpcDialOptions()...)

		trace.dialDone(ctx, s, err)

		return &conn{
			conn: gc,
			addr: addr,
		}, err
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
		dial: dial,
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
	curr, err = clusterUpsert(ctx, c, curr, nil)
	if err != nil {
		return nil, err
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
				// TODO: log add error.
				next, err := discover(ctx)
				if err != nil {
					return
				}
				sortEndpoints(curr)
				sortEndpoints(next)
				diffEndpoints(curr, next,
					func(i, j int) { // equal
						// do nothing.
					},
					func(i, j int) { // add
						// do nothing.
					},
					func(i, j int) { // del
						e := curr[i]
						addr := connAddr{
							addr: e.Addr,
							port: e.Port,
						}
						if !c.Remove(addr) {
							panic("ydb: driver can not delete endpoint")
						}
					},
				)
				curr, err = clusterUpsert(ctx, c, next, curr[:0])
				if err != nil {
					// log error
				}
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

// clusterUpsert calls Upsert() on given cluster with given context append
// every successful endpoint to dst slice.
// It returns resulting dst slice and a last error if and only if dst is empty.
func clusterUpsert(ctx context.Context, c *cluster, src, dst []Endpoint) ([]Endpoint, error) {
	var err error
	for _, e := range src {
		err = c.Upsert(ctx, e)
		if err == nil {
			dst = append(dst, e)
		}
	}
	return dst, err
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
	if t := d.requestTimeout; t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}
	conn, err := d.cluster.Get(ctx)
	if err != nil {
		return err
	}
	method, req, res := internal.Unwrap(op)
	return (grpcCaller{
		meta:  d.meta,
		trace: d.trace,
	}).call(ctx, conn, method, req, res)
}

func (d *driver) StreamRead(ctx context.Context, op internal.StreamOperation) error {
	if t := d.streamTimeout; t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}
	conn, err := d.cluster.Get(ctx)
	if err != nil {
		return err
	}
	method, req, res, processor := internal.UnwrapStreamOperation(op)
	return (grpcCaller{
		meta:  d.meta,
		trace: d.trace,
	}).streamRead(
		ctx, conn, method,
		req, res, processor,
	)
}

func Dial(ctx context.Context, addr string, c *DriverConfig) (Driver, error) {
	d := Dialer{
		DriverConfig: c,
	}
	return d.Dial(ctx, addr)
}

type grpcCaller struct {
	meta  *meta
	opts  []grpc.CallOption
	trace DriverTrace
}

func (g grpcCaller) streamRead(
	ctx context.Context, conn *conn, method string,
	req, res proto.Message, process func(error),
) (
	err error,
) {
	desc := grpc.StreamDesc{
		StreamName:    path.Base(method),
		ServerStreams: true,
	}
	s, err := grpc.NewClientStream(ctx, &desc, conn.conn, method)
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
			err = s.RecvMsg(res)
			process(err)
		}
	}()
	return nil
}

func (g grpcCaller) call(ctx context.Context, conn *conn, method string, req, res proto.Message) (err error) {
	var resp Ydb_Operations.GetOperationResponse

	g.trace.operationStart(ctx, conn, method)
	defer func(ctx context.Context) {
		g.trace.operationDone(ctx, conn, method, resp, err)
	}(ctx)

	md, err := g.meta.md(ctx)
	if err != nil {
		return err
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	err = grpc.Invoke(ctx, method, req, &resp, conn.conn)
	if err != nil {
		return mapGRPCError(err)
	}
	op := resp.Operation
	if !op.Ready {
		// TODO: implement awaiting.
		return ErrOperationNotReady
	}
	if op.Status != Ydb.StatusIds_SUCCESS {
		return &OpError{
			Reason: statusCode(op.Status),
			issues: op.Issues,
		}
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
