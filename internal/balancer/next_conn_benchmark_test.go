package balancer

import (
	"context"
	"strconv"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

var nextConnBenchmarkSink conn.Conn

func BenchmarkNextConn(b *testing.B) {
	tests := []struct {
		name      string
		nodeCount int
		balancer  func() config.Option
	}{
		{
			name:      "RandomChoice",
			nodeCount: 1,
			balancer: func() config.Option {
				return config.WithBalancer(balancers.RandomChoice())
			},
		},
		{
			name:      "RandomChoice",
			nodeCount: 10,
			balancer: func() config.Option {
				return config.WithBalancer(balancers.RandomChoice())
			},
		},
		{
			name:      "RandomChoice",
			nodeCount: 1000,
			balancer: func() config.Option {
				return config.WithBalancer(balancers.RandomChoice())
			},
		},
		{
			name:      "PreferWithFallback",
			nodeCount: 1000,
			balancer: func() config.Option {
				return config.WithBalancer(balancers.PreferWithFallback(
					balancers.RandomChoice(),
					func(candidate balancers.Endpoint) bool {
						return candidate.NodeID()%2 == 0
					},
				))
			},
		},
	}

	for _, test := range tests {
		b.Run(test.name+"/"+strconv.Itoa(test.nodeCount), func(b *testing.B) {
			benchmarkNextConn(b, test.nodeCount, test.balancer())
		})
	}
}

func benchmarkNextConn(b *testing.B, nodeCount int, balancerOption config.Option) {
	nodeIDs := make([]uint32, nodeCount)
	for i := range nodeIDs {
		nodeIDs[i] = uint32(i + 1)
	}
	discovery := startDynamicDiscoveryServer(b, nodeIDs)
	ctx := context.Background()
	cfg := config.New(
		config.WithEndpoint(discovery.endpoint()),
		config.WithDatabase("/benchmark"),
		config.WithGrpcOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		balancerOption,
	)
	pool := conn.NewPool(ctx, cfg)
	balancer, err := New(ctx, cfg, pool)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := balancer.Close(ctx); err != nil {
			b.Error(err)
		}
		if err := pool.RemoveRef(ctx); err != nil {
			b.Error(err)
		}
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nextConnBenchmarkSink, err = balancer.nextConn(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}
