package dial

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	public "github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Dial dials given addr and initializes driver instance on success.
func Dial(ctx context.Context, c config.Config) (_ public.Cluster, err error) {
	grpcKeepalive := c.GrpcConnectionPolicy().Timeout
	if grpcKeepalive <= 0 {
		grpcKeepalive = config.MinKeepaliveInterval
	}
	return (&dialer{
		netDial:   c.NetDial(),
		tlsConfig: c.TLSConfig(),
		timeout:   c.DialTimeout(),
		keepalive: grpcKeepalive,
		config:    c,
		meta: meta.New(
			c.Database(),
			c.Credentials(),
			c.Trace(),
			c.RequestsType(),
		),
	}).connect(ctx, c.Endpoint())
}

// dialer is an instance holding single Dialer.Dial() configuration parameters.
type dialer struct {
	netDial   func(context.Context, string) (net.Conn, error)
	tlsConfig *tls.Config
	timeout   time.Duration
	keepalive time.Duration
	config    config.Config
	meta      meta.Meta
}

func (d *dialer) connect(ctx context.Context, address string) (_ public.Cluster, err error) {
	c := d.newCluster(d.config.Trace())
	defer func() {
		if err != nil {
			_ = c.Close(ctx)
		}
	}()
	runtimeHolder := driver.New(
		d.config,
		d.meta,
		d.tlsConfig != nil,
		c.Get,
		c.Pessimize,
		c.Close,
	)
	if d.config.DiscoveryInterval() > 0 {
		if err := d.discover(
			ctx,
			c,
			conn.New(ctx, address, trace.LocationUnknown, d.dial, runtimeHolder),
			runtimeHolder,
		); err != nil {
			return nil, err
		}
	} else {
		c.Insert(ctx, address, cluster.WithConnConfig(runtimeHolder))
	}
	return runtimeHolder, nil
}

func (d *dialer) dial(ctx context.Context, address string) (_ *grpc.ClientConn, err error) {
	if d.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}
	return grpc.DialContext(ctx, address, d.grpcDialOptions()...)
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
		grpc.WithKeepaliveParams(d.config.GrpcConnectionPolicy().ClientParameters),
	)
	opts = append(opts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(config.DefaultGRPCMsgSize),
		grpc.MaxCallSendMsgSize(config.DefaultGRPCMsgSize),
	))

	return append(opts, grpc.WithBlock())
}

func (d *dialer) useTLS() bool {
	return d.tlsConfig != nil
}
