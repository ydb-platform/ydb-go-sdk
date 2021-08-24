package ydb

import (
	"context"
	"crypto/tls"
	"github.com/YandexDatabase/ydb-go-genproto/Ydb_Discovery_V1"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

var (
	// DefaultKeepaliveInterval contains default duration between grpc keepalive
	DefaultKeepaliveInterval = 5 * time.Minute
	MinKeepaliveInterval     = 1 * time.Minute
	DefaultGRPCMsgSize       = 64 * 1024 * 1024 // 64MB
)

func Dial(ctx context.Context, addr string, c *DriverConfig) (Driver, error) {
	d := Dialer{
		DriverConfig: c,
	}
	return d.Dial(ctx, addr)
}

// Dialer contains options of dialing and initialization of particular ydb
// driver.
type Dialer struct {
	// DriverConfig is a driver configuration.
	DriverConfig *DriverConfig

	// TLSConfig specifies the TLS configuration to use for tls client.
	// If TLSConfig is zero then connections are insecure.
	TLSConfig *tls.Config

	// Timeout is the maximum amount of time a dial will wait for a connect to
	// complete.
	// If Timeout is zero then no timeout is used.
	Timeout time.Duration

	// Keepalive is the interval used to check whether inner connections are
	// still valid.
	// Dialer could increase keepalive interval if given value is too small.
	Keepalive time.Duration

	// NetDial is an optional function that may replace default network dialing
	// function such as net.Dial("tcp").
	// Deprecated: Use it for test purposes and special cases only. In most cases should be left empty.
	NetDial func(context.Context, string) (net.Conn, error)
}

// Dial dials given addr and initializes driver instance on success.
func (d *Dialer) Dial(ctx context.Context, addr string) (_ *driver, err error) {
	config := d.DriverConfig.withDefaults()
	grpcKeepalive := d.Keepalive
	if grpcKeepalive == 0 {
		grpcKeepalive = DefaultKeepaliveInterval
	} else if grpcKeepalive < MinKeepaliveInterval {
		grpcKeepalive = MinKeepaliveInterval
	}
	tlsConfig := d.TLSConfig
	if tlsConfig != nil {
		tlsConfig.RootCAs, err = WithYdbCA(tlsConfig.RootCAs)
		if err != nil {
			return nil, err
		}
	}
	return (&dialer{
		netDial:   d.NetDial,
		tlsConfig: tlsConfig,
		keepalive: grpcKeepalive,
		timeout:   d.Timeout,
		config:    config,
		meta: &meta{
			trace:        config.Trace,
			database:     config.Database,
			credentials:  config.Credentials,
			requestsType: config.RequestsType,
		},
	}).dial(ctx, addr)
}

// dialer is an instance holding single Dialer.Dial() configuration parameters.
type dialer struct {
	netDial   func(context.Context, string) (net.Conn, error)
	tlsConfig *tls.Config
	keepalive time.Duration
	timeout   time.Duration
	config    DriverConfig
	meta      *meta
}

func (d *dialer) dial(ctx context.Context, addr string) (_ *driver, err error) {
	cluster := cluster{
		dial:  d.dialHostPort,
		trace: d.config.Trace,
	}
	defer func() {
		if err != nil {
			_ = cluster.Close()
		}
	}()
	driver := &driver{
		cluster:              &cluster,
		meta:                 d.meta,
		trace:                d.config.Trace,
		requestTimeout:       d.config.RequestTimeout,
		streamTimeout:        d.config.StreamTimeout,
		operationTimeout:     d.config.OperationTimeout,
		operationCancelAfter: d.config.OperationCancelAfter,
	}
	if d.config.DiscoveryInterval > 0 {
		if d.config.PreferLocalEndpoints {
			cluster.balancer = newMultiBalancer(
				withBalancer(
					d.newBalancer(), func(_ *conn, info connInfo) bool {
						return info.local
					},
				),
				withBalancer(
					d.newBalancer(), func(_ *conn, info connInfo) bool {
						return !info.local
					},
				),
			)
		} else {
			cluster.balancer = d.newBalancer()
		}

		var curr []Endpoint
		curr, err = d.discover(ctx, addr)
		if err != nil {
			return nil, err
		}
		// Endpoints must be sorted to merge
		sortEndpoints(curr)
		wg := newWG()
		wg.Add(len(curr))
		for _, e := range curr {
			go cluster.Insert(ctx, e, wg)
		}
		if d.config.FastDial {
			wg.WaitFirst()
		} else {
			wg.Wait()
		}
		discoveryClient := &discoveryClient{
			discoveryService: Ydb_Discovery_V1.NewDiscoveryServiceClient(driver.GetLazy()),
			database:         d.config.Database,
			ssl:              d.useTLS(),
		}
		cluster.explorer = NewRepeater(d.config.DiscoveryInterval, 0,
			func(ctx context.Context) {
				next, err := discoveryClient.Discover(ctx)
				if err != nil {
					return
				}
				// if nothing endpoint - re-discover after one second
				// and use old endpoint list
				if len(next) == 0 {
					go func() {
						time.Sleep(time.Second)
						cluster.explorer.Force()

					}()
					return
				}
				// NOTE: curr endpoints must be sorted here.
				sortEndpoints(next)

				wg := new(sync.WaitGroup)
				max := len(next) + len(curr)
				wg.Add(max) // set to max possible amount
				actual := 0
				diffEndpoints(curr, next,
					func(i, j int) {
						actual++
						// Endpoints are equal, but we still need to update meta
						// data such that load factor and others.
						go cluster.Update(ctx, next[j], wg)
					},
					func(i, j int) {
						actual++
						go cluster.Insert(ctx, next[j], wg)
					},
					func(i, j int) {
						actual++
						go cluster.Remove(ctx, curr[i], wg)
					},
				)
				wg.Add(actual - max) // adjust
				wg.Wait()
				curr = next
			})
	} else {
		var (
			e   Endpoint
			err error
		)
		e.Addr, e.Port, err = d.splitHostPort(addr)
		if err != nil {
			return nil, err
		}

		cluster.balancer = new(singleConnBalancer)
		cluster.Insert(ctx, e)

		// Ensure that endpoint is online.
		_, err = cluster.Get(ctx)
		if err != nil {
			return nil, err
		}
	}
	return driver, nil
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
	driverTraceDialDone := driverTraceOnDial(ctx, d.config.Trace, ctx, s)

	cc, err := grpc.DialContext(ctx, s, d.grpcDialOptions()...)

	driverTraceDialDone(rawctx, s, err)
	if err != nil {
		return nil, err
	}

	return newConn(cc, addr), nil
}

func (d *dialer) dialAddr(ctx context.Context, addr string) (*conn, error) {
	host, port, err := d.splitHostPort(addr)
	if err != nil {
		return nil, err
	}
	return d.dialHostPort(ctx, host, port)
}

func (d *dialer) discover(ctx context.Context, addr string) (endpoints []Endpoint, err error) {
	driverTraceDiscoveryDone := driverTraceOnDiscovery(ctx, d.config.Trace, ctx)
	defer func() {
		driverTraceDiscoveryDone(ctx, endpoints, err)
	}()

	var c *conn
	c, err = d.dialAddr(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = c.raw.Close()
	}()

	var cancel context.CancelFunc
	if d.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, d.timeout)
	}
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	return discover(
		ctx,
		Ydb_Discovery_V1.NewDiscoveryServiceClient(&grpcConn{
			c: c,
			d: &driver{
				meta: d.meta,
			},
		}),
		d.config.Database,
		d.useTLS(),
	)
}

func (d *dialer) grpcDialOptions() (opts []grpc.DialOption) {
	if d.netDial != nil {
		opts = append(opts, grpc.WithContextDialer(d.netDial))
	}
	if d.useTLS() {
		opts = append(opts, grpc.WithTransportCredentials(
			credentials.NewTLS(d.tlsConfig),
		))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	opts = append(opts,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    d.keepalive,
			Timeout: d.timeout,
		}),
	)
	opts = append(opts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(DefaultGRPCMsgSize),
		grpc.MaxCallSendMsgSize(DefaultGRPCMsgSize),
	))

	return append(opts, grpc.WithBlock())
}

func (d *dialer) newBalancer() balancer {
	return balancers[d.config.BalancingMethod](d.config.BalancingConfig)
}

func (d *dialer) useTLS() bool {
	return d.tlsConfig != nil
}

func (d *dialer) splitHostPort(addr string) (host string, port int, err error) {
	var prt string
	host, prt, err = net.SplitHostPort(addr)
	if err != nil {
		return
	}
	port, err = strconv.Atoi(prt)
	return
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
