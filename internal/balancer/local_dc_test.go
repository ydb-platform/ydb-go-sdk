package balancer

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

var localIP = net.IPv4(127, 0, 0, 1)

type discoveryMock struct {
	endpoints []endpoint.Endpoint
}

// implement discovery.Client
func (d discoveryMock) Close(ctx context.Context) error {
	return nil
}

func (d discoveryMock) Discover(ctx context.Context) ([]endpoint.Endpoint, error) {
	return d.endpoints, nil
}

func TestCheckFastestAddress(t *testing.T) {
	ctx := context.Background()

	t.Run("Ok", func(t *testing.T) {
		var firstCount int64
		var secondCount int64

		for i := 0; i < 100; i++ {
			listen1, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
			require.NoError(t, err)
			listen2, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
			require.NoError(t, err)
			addr1 := listen1.Addr().String()
			addr2 := listen2.Addr().String()

			fastest := checkFastestAddress(ctx, []string{addr1, addr2})
			require.NotEmpty(t, fastest)

			switch fastest {
			case addr1:
				firstCount++
			case addr2:
				secondCount++
			default:
				require.Contains(t, []string{addr1, addr2}, fastest)
			}

			_ = listen1.Close()
			_ = listen2.Close()
		}
		require.NotEmpty(t, firstCount)
		require.NotEmpty(t, secondCount)
	})
	t.Run("HasError", func(t *testing.T) {
		listen1, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
		require.NoError(t, err)
		listen2, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
		require.NoError(t, err)
		addr1 := listen1.Addr().String()
		addr2 := listen2.Addr().String()

		_ = listen2.Close() // for can't accept connections

		fastest := checkFastestAddress(ctx, []string{addr1, addr2})
		require.Equal(t, addr1, fastest)

		_ = listen1.Close()
	})
	t.Run("AllErrors", func(t *testing.T) {
		listen1, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
		require.NoError(t, err)
		listen2, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
		require.NoError(t, err)
		addr1 := listen1.Addr().String()
		addr2 := listen2.Addr().String()

		_ = listen1.Close() // for can't accept connections
		_ = listen2.Close() // for can't accept connections

		res := checkFastestAddress(ctx, []string{addr1, addr2})
		require.Empty(t, res)
	})
}

func TestDetectLocalDC(t *testing.T) {
	ctx := context.Background()
	xtest.TestManyTimesWithName(t, "Ok", func(t testing.TB) {
		listen1, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
		require.NoError(t, err)
		defer func() { _ = listen1.Close() }()

		listen2, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
		require.NoError(t, err)
		listen2Addr := listen2.Addr().String()
		_ = listen2.Close() // force close, for not accept tcp connections

		dc, err := detectLocalDC(ctx, []endpoint.Endpoint{
			&mock.Endpoint{LocationField: "a", AddrField: "grpc://" + listen1.Addr().String()},
			&mock.Endpoint{LocationField: "b", AddrField: "grpc://" + listen2Addr},
		})
		require.NoError(t, err)
		require.Equal(t, "a", dc)
	})
	t.Run("Empty", func(t *testing.T) {
		res, err := detectLocalDC(ctx, nil)
		require.Equal(t, "", res)
		require.Error(t, err)
	})
	t.Run("OneDC", func(t *testing.T) {
		res, err := detectLocalDC(ctx, []endpoint.Endpoint{
			&mock.Endpoint{LocationField: "a"},
			&mock.Endpoint{LocationField: "a"},
		})
		require.NoError(t, err)
		require.Equal(t, "a", res)
	})
}

func TestLocalDCDiscovery(t *testing.T) {
	ctx := context.Background()
	cfg := config.New(
		config.WithBalancer(balancers.PreferNearestDC(balancers.Default())),
	)
	r := &Balancer{
		driverConfig:   cfg,
		balancerConfig: *cfg.Balancer(),
		pool:           conn.NewPool(context.Background(), cfg),
		discover: func(ctx context.Context, _ *grpc.ClientConn) (endpoints []endpoint.Endpoint, location string, err error) {
			return []endpoint.Endpoint{
				&mock.Endpoint{AddrField: "a:123", LocationField: "a"},
				&mock.Endpoint{AddrField: "b:234", LocationField: "b"},
				&mock.Endpoint{AddrField: "c:456", LocationField: "c"},
			}, "", nil
		},
		localDCDetector: func(ctx context.Context, endpoints []endpoint.Endpoint) (string, error) {
			return "b", nil
		},
	}

	err := r.clusterDiscoveryAttempt(ctx, nil)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		conn, _ := r.connections().GetConnection(ctx)
		require.Equal(t, "b:234", conn.Endpoint().Address())
		require.Equal(t, "b", conn.Endpoint().Location())
	}
}

func TestExtractHostPort(t *testing.T) {
	table := []struct {
		name    string
		address string
		host    string
		port    string
		err     bool
	}{
		{
			"HostPort",
			"asd:123",
			"asd",
			"123",
			false,
		},
		{
			"HostPortSchema",
			"grpc://asd:123",
			"asd",
			"123",
			false,
		},
		{
			"NoPort",
			"host",
			"",
			"",
			true,
		},
		{
			"Empty",
			"",
			"",
			"",
			true,
		},
	}
	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			host, port, err := extractHostPort(test.address)
			require.Equal(t, test.host, host)
			require.Equal(t, test.port, port)
			if test.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetRandomEndpoints(t *testing.T) {
	source := []endpoint.Endpoint{
		&mock.Endpoint{AddrField: "a"},
		&mock.Endpoint{AddrField: "b"},
		&mock.Endpoint{AddrField: "c"},
	}

	t.Run("ReturnSource", func(t *testing.T) {
		res := getRandomEndpoints(source, 3)
		require.Equal(t, source, res)

		res = getRandomEndpoints(source, 4)
		require.Equal(t, source, res)
	})
	xtest.TestManyTimesWithName(t, "SelectRandom", func(t testing.TB) {
		res := getRandomEndpoints(source, 2)
		require.Len(t, res, 2)
		for _, ep := range res {
			require.Contains(t, source, ep)
		}
		require.NotEqual(t, res[0], res[1])
	})
}
