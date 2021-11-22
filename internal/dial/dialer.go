package dial

import (
	"context"
	"crypto/tls"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	public "github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
)

// Dial dials given addr and initializes driver instance on success.
func Dial(ctx context.Context, c config.Config) (_ public.Cluster, err error) {
	return (&dialer{
		netDial:   c.NetDial(),
		tlsConfig: c.TLSConfig(),
		config:    c,
		meta: meta.New(
			c.Database(),
			c.Credentials(),
			c.Trace(),
			c.RequestsType(),
		),
	}).connect(ctx, endpoint.New(c.Endpoint()))
}

// dialer is an instance holding single Dialer.Dial() configuration parameters.
type dialer struct {
	netDial   func(context.Context, string) (net.Conn, error)
	tlsConfig *tls.Config
	config    config.Config
	meta      meta.Meta
}

func (d *dialer) connect(ctx context.Context, endpoint endpoint.Endpoint) (_ public.Cluster, err error) {
	c := d.newCluster(d.config.Trace())
	defer func() {
		if err != nil {
			_ = c.Close(ctx)
		}
	}()
	driver := driver.New(
		d.config,
		d.meta,
		c.Get,
		c.Pessimize,
		c.Close,
	)
	if d.config.DiscoveryInterval() > 0 {
		if err := d.discover(
			ctx,
			c,
			conn.New(endpoint, d.dial, driver),
			driver,
		); err != nil {
			return nil, err
		}
	} else {
		c.Insert(ctx, endpoint, cluster.WithConnConfig(driver))
	}
	return driver, nil
}

func (d *dialer) dial(ctx context.Context, address string) (_ *grpc.ClientConn, err error) {
	return grpc.DialContext(ctx, address, d.grpcDialOptions()...)
}

func (d *dialer) grpcDialOptions() (opts []grpc.DialOption) {
	if d.netDial != nil {
		opts = append(opts, grpc.WithContextDialer(d.netDial))
	} else {
		netDial := func(ctx context.Context, address string) (net.Conn, error) {
			return newConn(ctx, address, d.config.Trace())
		}
		opts = append(opts, grpc.WithContextDialer(netDial))
	}
	if d.config.Secure() {
		opts = append(opts, grpc.WithTransportCredentials(
			credentials.NewTLS(d.tlsConfig),
		))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	opts = append(opts,
		grpc.WithKeepaliveParams(d.config.GrpcConnectionPolicy().ClientParameters),
		grpc.WithDefaultServiceConfig(`{
			"loadBalancingConfig": [
				{
					"round_robin":{}
				}
			],
			"loadBalancingPolicy":"round_robin"
		}`),
	)
	opts = append(opts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(config.DefaultGRPCMsgSize),
		grpc.MaxCallSendMsgSize(config.DefaultGRPCMsgSize),
	))

	return append(opts, grpc.WithBlock())
}
