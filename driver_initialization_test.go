package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestFailedDriverInitializationReleasesResources(t *testing.T) {
	for _, constructor := range []struct {
		name string
		open func(context.Context, string, ...Option) (*Driver, error)
	}{
		{"Open", Open},
		{"New", func(ctx context.Context, dsn string, opts ...Option) (*Driver, error) {
			return New(ctx, append([]Option{WithConnectionString(dsn)}, opts...)...)
		}},
	} {
		for _, failure := range []string{"configuration", "balancer", "canceled"} {
			t.Run(constructor.name+"/"+failure, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				var created *Driver
				var driverContext, releaseContext context.Context
				opts := []Option{
					WithBalancer(balancers.RandomChoice()),
					WithDiscoveryInterval(-1),
					WithTraceDriver(trace.Driver{
						OnBalancerInit: func(trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
							if failure == "canceled" {
								cancel()
							}

							return nil
						},
						OnPoolRelease: func(info trace.DriverConnPoolReleaseStartInfo) func(trace.DriverConnPoolReleaseDoneInfo) {
							releaseContext = *info.Context

							return nil
						},
					}),
					func(ctx context.Context, d *Driver) error {
						created, driverContext = d, ctx

						return nil
					},
				}
				dsn := "grpc://localhost:2135/missing"
				if failure == "configuration" {
					dsn = "grpc://localhost:2135"
				}

				driver, err := constructor.open(ctx, dsn, opts...)
				require.Error(t, err)
				require.Nil(t, driver)
				t.Cleanup(func() {
					created.ctxCancel()
					if created.pool != nil {
						_ = created.pool.RemoveRef(context.Background())
					}
				})
				require.ErrorIs(t, driverContext.Err(), context.Canceled)
				if failure == "configuration" {
					require.Nil(t, created.pool)

					return
				}
				require.NotNil(t, releaseContext)
				require.NoError(t, releaseContext.Err())
				require.ErrorIs(t, created.pool.AddRef(t.Context()), conn.ErrClosedPool)
				if failure == "canceled" {
					require.ErrorIs(t, ctx.Err(), context.Canceled)
				}
			})
		}
	}
}
