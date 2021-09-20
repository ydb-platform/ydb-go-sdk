package dial

import (
	"context"
	"crypto/tls"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pem"
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	// DefaultKeepaliveInterval contains default duration between grpc keepalive
	DefaultKeepaliveInterval = 5 * time.Minute
	MinKeepaliveInterval     = 1 * time.Minute
	DefaultGRPCMsgSize       = 64 * 1024 * 1024 // 64MB
)

// Dialer contains options of dialing and initialization of particular ydb
// driver.
type Dialer struct {
	// DriverConfig is a driver configuration.
	DriverConfig *config.Config

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
func (d *Dialer) Dial(ctx context.Context, addr string) (_ cluster.Cluster, err error) {
	config := d.DriverConfig.WithDefaults()
	grpcKeepalive := d.Keepalive
	if grpcKeepalive == 0 {
		grpcKeepalive = DefaultKeepaliveInterval
	} else if grpcKeepalive < MinKeepaliveInterval {
		grpcKeepalive = MinKeepaliveInterval
	}
	tlsConfig := d.TLSConfig
	if tlsConfig != nil {
		tlsConfig.RootCAs, err = pem.WithYdbCA(tlsConfig.RootCAs)
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
		meta: meta.New(
			config.Database,
			config.Credentials,
			config.Trace,
			config.RequestsType,
		),
	}).dial(ctx, addr)
}

// dialer is an instance holding single Dialer.Dial() configuration parameters.
type dialer struct {
	netDial   func(context.Context, string) (net.Conn, error)
	tlsConfig *tls.Config
	keepalive time.Duration
	timeout   time.Duration
	config    config.Config
	meta      meta.Meta
}

func (d *dialer) dial(ctx context.Context, addr string) (_ cluster.Cluster, err error) {
	endpoint := d.endpointByAddr(addr)
	c := d.newCluster()
	defer func() {
		if err != nil {
			_ = c.Close()
		}
	}()
	driver := driver.New(
		d.config,
		d.meta,
		c.Get,
		c.Pessimize,
		c.Stats,
		c.Close,
	)
	// Ensure that endpoint is online.
	var conn *grpc.ClientConn
	conn, err = d.dialHostPort(ctx, endpoint.Addr, endpoint.Port)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()
	if err != nil {
		return nil, err
	}
	if d.config.DiscoveryInterval > 0 {
		if err := d.discover(ctx, c, conn, driver); err != nil {
			return nil, err
		}
	}
	return driver, nil
}

func (d *dialer) dialHostPort(ctx context.Context, host string, port int) (*grpc.ClientConn, error) {
	if d.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}
	s := cluster.String(host, port)
	t := trace.ContextDriverTrace(ctx).Compose(d.config.Trace)
	var dialDone func(trace.DialDoneInfo)
	if t.OnDial != nil {
		dialDone = t.OnDial(trace.DialStartInfo{
			Context: ctx,
			Address: s,
		})
	}

	cc, err := grpc.DialContext(ctx, s, d.grpcDialOptions()...)

	if dialDone != nil {
		dialDone(trace.DialDoneInfo{
			Error: err,
		})
	}
	if err != nil {
		return nil, err
	}

	return cc, nil
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

func (d *dialer) mustSplitHostPort(addr string) (host string, port int) {
	host, prt, err := net.SplitHostPort(addr)
	if err != nil {
		panic(err)
	}
	port, err = strconv.Atoi(prt)
	if err != nil {
		panic(err)
	}
	return host, port
}

func (d *dialer) endpointByAddr(addr string) (e cluster.Endpoint) {
	e.Addr, e.Port = d.mustSplitHostPort(addr)
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
