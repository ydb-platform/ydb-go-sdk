//go:build !fast
// +build !fast

package discovery_test

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
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestDiscovery(t *testing.T) {
	var (
		userAgent     = "connection user agent"
		requestType   = "connection request type"
		checkMedatada = func(ctx context.Context) {
			md, has := metadata.FromOutgoingContext(ctx)
			if !has {
				t.Fatalf("no medatada")
			}
			userAgents := md.Get(meta.HeaderUserAgent)
			if len(userAgents) == 0 {
				t.Fatalf("no user agent")
			}
			if userAgents[0] != userAgent {
				t.Fatalf("unknown user agent: %s", userAgents[0])
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
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	db, err := ydb.Open(
		ctx,
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
			trace.MatchDetails(`ydb\.(driver|discovery).*`),
			ydb.WithNamespace("ydb"),
			ydb.WithOutWriter(os.Stdout),
			ydb.WithErrWriter(os.Stdout),
			ydb.WithMinLevel(log.WARN),
		),
		ydb.WithUserAgent(userAgent),
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
	t.Run("immediate", func(t *testing.T) {
		if _, err = db.Discovery().Discover(ctx); err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
	})
	t.Run("after parking", func(t *testing.T) {
		<-parking // wait for parking conn
		if _, err = db.Discovery().Discover(ctx); err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
	})
}
