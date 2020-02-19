package ydb

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

var (
	// DefaultKeepaliveInterval contains default duration between grpc keepalive
	DefaultKeepaliveInterval = 10 * time.Second
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

	// Keepalive is the interval used to check whether inner connections are
	// still valid.
	// If Keepalive is zero then there will be no keepalive checks.
	//
	// Dialer could increase keepalive interval if given value is too small.
	Keepalive time.Duration
}

// Dial dials given addr and initializes driver instance on success.
func (d *Dialer) Dial(ctx context.Context, addr string) (Driver, error) {
	config := d.DriverConfig.withDefaults()
	grpcKeepalive := d.Keepalive
	if grpcKeepalive == 0 {
		grpcKeepalive = DefaultKeepaliveInterval
	}

	return (&dialer{
		netDial:   d.NetDial,
		tlsConfig: d.TLSConfig,
		keepalive: grpcKeepalive,
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
	keepalive time.Duration
	timeout   time.Duration
	config    DriverConfig
	meta      *meta
}

func (d *dialer) dial(ctx context.Context, addr string) (_ Driver, err error) {
	cluster := cluster{
		dial:  d.dialHostPort,
		trace: d.config.Trace,
	}
	defer func() {
		if err != nil {
			_ = cluster.Close()
		}
	}()
	var explorer *repeater
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

		// Ensure that endpoint is online.
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
	d.config.Trace.discoveryStart(ctx)
	defer func() {
		d.config.Trace.discoveryDone(ctx, endpoints, err)
	}()

	conn, err := d.dialAddr(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.conn.Close()

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
		opts = append(opts, grpc.WithContextDialer(d.netDial))
	}
	if c := d.tlsConfig; c != nil {
		opts = append(opts, grpc.WithTransportCredentials(
			credentials.NewTLS(c),
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
	return append(opts, grpc.WithBlock())
}

func (d *dialer) newBalancer() balancer {
	return balancers[d.config.BalancingMethod](d.config.BalancingConfig)
}
