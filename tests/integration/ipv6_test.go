//go:build integration
// +build integration

package integration

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestOnlyIPVersion(t *testing.T) {
	scope := newScope(t)
	ctx, cancel := context.WithTimeout(scope.Ctx, time.Second)
	defer cancel()

	resolutions := make(chan []string, 1)
	// The bootstrap resolver update is delivered while ydb.Open is blocked. Canceling it avoids waiting for
	// the IPv6 connection attempt to time out against the IPv4-only Docker YDB instance.
	opts := []ydb.Option{
		ydb.WithBalancer(balancers.OnlyIPVersion(balancers.RandomChoice(), balancers.IPv6)),
		ydb.WithTraceDriver(trace.Driver{
			OnResolve: func(info trace.DriverResolveStartInfo) func(trace.DriverResolveDoneInfo) {
				return func(trace.DriverResolveDoneInfo) {
					select {
					case resolutions <- info.Resolved:
						cancel()
					default:
					}
				}
			},
		}),
	}
	if token := scope.AuthToken(); token == "" {
		opts = append(opts, ydb.WithAnonymousCredentials())
	} else {
		opts = append(opts, ydb.WithAccessTokenCredentials(token))
	}
	if cert := scope.CertFile(); cert == "" {
		opts = append(opts, ydb.WithTLSSInsecureSkipVerify())
	} else {
		opts = append(opts, ydb.WithCertificatesFromFile(cert))
	}

	db, err := ydb.Open(ctx, scope.ConnectionString(), opts...)

	require.Nil(t, db)
	require.ErrorIs(t, err, context.Canceled)

	select {
	case addresses := <-resolutions:
		require.NotEmpty(t, addresses)
		for _, address := range addresses {
			host, _, splitErr := net.SplitHostPort(address)
			require.NoError(t, splitErr)

			ip, parseErr := netip.ParseAddr(host)
			require.NoError(t, parseErr)
			require.True(t, ip.Is6() && !ip.Is4In6())
		}
	default:
		t.Fatal("resolver update was not traced")
	}
}
