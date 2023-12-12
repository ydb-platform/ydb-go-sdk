//go:build integration
// +build integration

package integration

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Export_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scripting_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Export"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scripting"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//nolint:gocyclo
func TestConnection(t *testing.T) {
	const sumColumn = "sum"
	var (
		userAgent     = "connection user agent"
		requestType   = "connection request type"
		checkMetadata = func(ctx context.Context) {
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
			traceIDs := md.Get(meta.HeaderTraceID)
			if len(traceIDs) == 0 {
				t.Fatalf("no traceIDs")
			}
			if len(traceIDs[0]) == 0 {
				t.Fatalf("no traceID")
			}
		}
		ctx = xtest.Context(t)
	)

	db, err := ydb.Open(ctx,
		"", // corner case for check replacement of endpoint+database+secure
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithAccessTokenCredentials(
			os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
		),
		ydb.With(
			config.WithOperationTimeout(time.Second*2),
			config.WithOperationCancelAfter(time.Second*2),
		),
		ydb.WithConnectionTTL(time.Millisecond*10000),
		ydb.WithMinTLSVersion(tls.VersionTLS10),
		ydb.WithLogger(
			newLoggerWithMinLevel(t, log.WARN),
			trace.MatchDetails(`ydb\.(driver|discovery|retry|scheme).*`),
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
					checkMetadata(ctx)
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
					checkMetadata(ctx)
					return streamer(ctx, desc, cc, method, opts...)
				}),
			),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// cleanup connection
		if e := db.Close(ctx); e != nil {
			t.Fatalf("close failed: %+v", e)
		}
	}()
	t.Run("discovery.WhoAmI", func(t *testing.T) {
		if err = retry.Retry(ctx, func(ctx context.Context) (err error) {
			discoveryClient := Ydb_Discovery_V1.NewDiscoveryServiceClient(ydb.GRPCConn(db))
			response, err := discoveryClient.WhoAmI(
				ctx,
				&Ydb_Discovery.WhoAmIRequest{IncludeGroups: true},
			)
			if err != nil {
				return err
			}
			var result Ydb_Discovery.WhoAmIResult
			err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
			if err != nil {
				return
			}
			return nil
		}, retry.WithIdempotent(true)); err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
	})
	t.Run("scripting.ExecuteYql", func(t *testing.T) {
		if err = retry.Retry(ctx, func(ctx context.Context) (err error) {
			scriptingClient := Ydb_Scripting_V1.NewScriptingServiceClient(ydb.GRPCConn(db))
			response, err := scriptingClient.ExecuteYql(
				ctx,
				&Ydb_Scripting.ExecuteYqlRequest{Script: "SELECT 1+100 AS sum"},
			)
			if err != nil {
				return err
			}
			var result Ydb_Scripting.ExecuteYqlResult
			err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
			if err != nil {
				return
			}
			if len(result.GetResultSets()) != 1 {
				return fmt.Errorf(
					"unexpected result sets count: %d",
					len(result.GetResultSets()),
				)
			}
			if len(result.GetResultSets()[0].GetColumns()) != 1 {
				return fmt.Errorf(
					"unexpected colums count: %d",
					len(result.GetResultSets()[0].GetColumns()),
				)
			}
			if result.GetResultSets()[0].GetColumns()[0].GetName() != sumColumn {
				return fmt.Errorf(
					"unexpected colum name: %s",
					result.GetResultSets()[0].GetColumns()[0].GetName(),
				)
			}
			if len(result.GetResultSets()[0].GetRows()) != 1 {
				return fmt.Errorf(
					"unexpected rows count: %d",
					len(result.GetResultSets()[0].GetRows()),
				)
			}
			if result.GetResultSets()[0].GetRows()[0].GetItems()[0].GetInt32Value() != 101 {
				return fmt.Errorf(
					"unexpected result of select: %d",
					result.GetResultSets()[0].GetRows()[0].GetInt64Value(),
				)
			}
			return nil
		}, retry.WithIdempotent(true)); err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
	})
	t.Run("scripting.StreamExecuteYql", func(t *testing.T) {
		if err = retry.Retry(ctx, func(ctx context.Context) (err error) {
			scriptingClient := Ydb_Scripting_V1.NewScriptingServiceClient(ydb.GRPCConn(db))
			client, err := scriptingClient.StreamExecuteYql(
				ctx,
				&Ydb_Scripting.ExecuteYqlRequest{Script: "SELECT 1+100 AS sum"},
			)
			if err != nil {
				return err
			}
			response, err := client.Recv()
			if err != nil {
				return err
			}
			if len(response.GetResult().GetResultSet().GetColumns()) != 1 {
				return fmt.Errorf(
					"unexpected colums count: %d",
					len(response.GetResult().GetResultSet().GetColumns()),
				)
			}
			if response.GetResult().GetResultSet().GetColumns()[0].GetName() != sumColumn {
				return fmt.Errorf(
					"unexpected colum name: %s",
					response.GetResult().GetResultSet().GetColumns()[0].GetName(),
				)
			}
			if len(response.GetResult().GetResultSet().GetRows()) != 1 {
				return fmt.Errorf(
					"unexpected rows count: %d",
					len(response.GetResult().GetResultSet().GetRows()),
				)
			}
			if response.GetResult().GetResultSet().GetRows()[0].GetItems()[0].GetInt32Value() != 101 {
				return fmt.Errorf(
					"unexpected result of select: %d",
					response.GetResult().GetResultSet().GetRows()[0].GetInt64Value(),
				)
			}
			return nil
		}, retry.WithIdempotent(true)); err != nil {
			t.Fatalf("Stream execute failed: %v", err)
		}
	})
	t.Run("with.scripting.StreamExecuteYql", func(t *testing.T) {
		var childDB *ydb.Driver
		childDB, err = db.With(
			ctx,
			ydb.WithDialTimeout(time.Second*5),
		)
		if err != nil {
			t.Fatalf("failed to open sub-connection: %v", err)
		}
		defer func() {
			_ = childDB.Close(ctx)
		}()
		if err = retry.Retry(ctx, func(ctx context.Context) (err error) {
			scriptingClient := Ydb_Scripting_V1.NewScriptingServiceClient(ydb.GRPCConn(childDB))
			client, err := scriptingClient.StreamExecuteYql(
				ctx,
				&Ydb_Scripting.ExecuteYqlRequest{Script: "SELECT 1+100 AS sum"},
			)
			if err != nil {
				return err
			}
			response, err := client.Recv()
			if err != nil {
				return err
			}
			if len(response.GetResult().GetResultSet().GetColumns()) != 1 {
				return fmt.Errorf(
					"unexpected colums count: %d",
					len(response.GetResult().GetResultSet().GetColumns()),
				)
			}
			if response.GetResult().GetResultSet().GetColumns()[0].GetName() != sumColumn {
				return fmt.Errorf(
					"unexpected colum name: %s",
					response.GetResult().GetResultSet().GetColumns()[0].GetName(),
				)
			}
			if len(response.GetResult().GetResultSet().GetRows()) != 1 {
				return fmt.Errorf(
					"unexpected rows count: %d",
					len(response.GetResult().GetResultSet().GetRows()),
				)
			}
			if response.GetResult().GetResultSet().GetRows()[0].GetItems()[0].GetInt32Value() != 101 {
				return fmt.Errorf(
					"unexpected result of select: %d",
					response.GetResult().GetResultSet().GetRows()[0].GetInt64Value(),
				)
			}
			return nil
		}, retry.WithIdempotent(true)); err != nil {
			t.Fatalf("Stream execute failed: %v", err)
		}
	})
	t.Run("export.ExportToS3", func(t *testing.T) {
		if err = retry.Retry(ctx, func(ctx context.Context) (err error) {
			exportClient := Ydb_Export_V1.NewExportServiceClient(ydb.GRPCConn(db))
			response, err := exportClient.ExportToS3(
				ctx,
				&Ydb_Export.ExportToS3Request{
					OperationParams: &Ydb_Operations.OperationParams{
						OperationTimeout: durationpb.New(time.Second),
						CancelAfter:      durationpb.New(time.Second),
					},
					Settings: &Ydb_Export.ExportToS3Settings{},
				},
			)
			if err != nil {
				return err
			}
			if response.GetOperation().GetStatus() != Ydb.StatusIds_BAD_REQUEST {
				return fmt.Errorf(
					"operation must be BAD_REQUEST: %s",
					response.GetOperation().GetStatus().String(),
				)
			}
			return nil
		}, retry.WithIdempotent(true)); err != nil {
			t.Fatalf("check export failed: %v", err)
		}
	})
}

func TestZeroDialTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	var traceID string

	db, err := ydb.Open(
		ctx,
		"grpc://non-existent.com:2135/some",
		ydb.WithDialTimeout(0),
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
					md, has := metadata.FromOutgoingContext(ctx)
					if !has {
						t.Fatalf("no medatada")
					}
					traceIDs := md.Get(meta.HeaderTraceID)
					if len(traceIDs) == 0 {
						t.Fatalf("no traceIDs")
					}
					traceID = traceIDs[0]
					return invoker(ctx, method, req, reply, cc, opts...)
				}),
			),
		),
	)

	require.Error(t, err)
	require.ErrorContains(t, err, traceID)
	require.Nil(t, db)
	if !ydb.IsTransportError(err, grpcCodes.DeadlineExceeded) {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

func TestClusterDiscoveryRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	counter := 0

	db, err := ydb.Open(ctx,
		"grpc://non-existent.com:2135/some",
		ydb.WithDialTimeout(time.Second),
		ydb.WithTraceDriver(trace.Driver{
			OnBalancerClusterDiscoveryAttempt: func(info trace.DriverBalancerClusterDiscoveryAttemptStartInfo) func(
				trace.DriverBalancerClusterDiscoveryAttemptDoneInfo,
			) {
				counter++
				return nil
			},
		}),
	)
	t.Logf("attempts: %d", counter)
	t.Logf("err: %v", err)
	require.Error(t, err)
	require.Nil(t, db)
	if !ydb.IsTransportError(err, grpcCodes.DeadlineExceeded) {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	require.Greater(t, counter, 1)
}
