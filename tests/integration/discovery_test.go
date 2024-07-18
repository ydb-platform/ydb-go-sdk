//go:build integration
// +build integration

package integration

import (
	"context"
	"crypto/tls"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestDiscovery(sourceTest *testing.T) {
	t := xtest.MakeSyncedTest(sourceTest)
	var (
		userAgent     = "connection user agent"
		requestType   = "connection request type"
		checkMedatada = func(ctx context.Context) {
			md, has := metadata.FromOutgoingContext(ctx)
			if !has {
				t.Fatalf("no medatada")
			}
			applicationName := md.Get(meta.HeaderApplicationName)
			if len(applicationName) == 0 {
				t.Fatalf("no application name")
			}
			if applicationName[0] != userAgent {
				t.Fatalf("unknown user agent: %s", applicationName[0])
			}
			requestTypes := md.Get(meta.HeaderRequestType)
			if len(requestTypes) == 0 {
				t.Fatalf("no request type")
			}
			if requestTypes[0] != requestType {
				t.Fatalf("unknown request type: %s", requestTypes[0])
			}
		}
		parking = make(chan struct{})
		ctx     = xtest.Context(t)
	)

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(
			os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
		),
		ydb.With(
			config.WithOperationTimeout(time.Second*2),
			config.WithOperationCancelAfter(time.Second*2),
		),
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.WithConnectionTTL(time.Second*1),
		ydb.WithMinTLSVersion(tls.VersionTLS10),
		ydb.WithLogger(
			newLoggerWithMinLevel(t, log.WARN),
			trace.MatchDetails(`ydb\.(driver|discovery|repeater).*`),
		),
		ydb.WithApplicationName(userAgent),
		ydb.WithRequestsType(requestType),
		ydb.With(
			config.WithGrpcOptions(
				grpc.WithUnaryInterceptor(func(
					ctx context.Context,
					method string,
					req, reply interface{},
					cc *grpc.ClientConn,
					invoker grpc.UnaryInvoker,
					opts ...grpc.CallOption,
				) error {
					checkMedatada(ctx)
					return invoker(ctx, method, req, reply, cc, opts...)
				}),
				grpc.WithStreamInterceptor(func(
					ctx context.Context,
					desc *grpc.StreamDesc,
					cc *grpc.ClientConn,
					method string,
					streamer grpc.Streamer,
					opts ...grpc.CallOption,
				) (grpc.ClientStream, error) {
					checkMedatada(ctx)
					return streamer(ctx, desc, cc, method, opts...)
				}),
			),
		),
		ydb.WithTraceDriver(trace.Driver{
			OnConnPark: func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
				return func(info trace.DriverConnParkDoneInfo) {
					parking <- struct{}{}
				}
			},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// cleanup connection
		if e := db.Close(ctx); e != nil {
			t.Fatalf("db close failed: %+v", e)
		}
	}()
	t.Run("discovery.Discover", func(t *testing.T) {
		if endpoints, err := db.Discovery().Discover(ctx); err != nil {
			t.Fatal(err)
		} else {
			t.Log(endpoints)
		}
		t.Run("wait", func(t *testing.T) {
			t.Run("parking", func(t *testing.T) {
				<-parking // wait for parking conn
				t.Run("re-discover", func(t *testing.T) {
					if endpoints, err := db.Discovery().Discover(ctx); err != nil {
						t.Fatal(err)
					} else {
						t.Log(endpoints)
					}
				})
			})
		})
	})
}
