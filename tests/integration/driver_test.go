//go:build integration
// +build integration

package integration

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"os"
	"path"
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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//nolint:gocyclo
func TestDriver(sourceTest *testing.T) {
	t := xtest.MakeSyncedTest(sourceTest)
	const sumColumn = "sum"
	var (
		userAgent     = "connection user agent"
		requestType   = "connection request type"
		traceParentID = "test-traceparent-id"
		checkMetadata = func(ctx context.Context) {
			md, has := metadata.FromOutgoingContext(ctx)
			if !has {
				t.Fatalf("no medatada")
			}
			userAgents := md.Get(meta.HeaderApplicationName)
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
				t.Fatalf("empty traceID header")
			}
			traceParent := md.Get(meta.HeaderTraceParent)
			if len(traceParent) == 0 {
				t.Fatalf("no traceparent header")
			}
			if len(traceParent[0]) == 0 {
				t.Fatalf("empty traceparent header")
			}
			if traceParent[0] != traceParentID {
				t.Fatalf("unexpected traceparent header")
			}
		}
		ctx = meta.WithTraceParent(xtest.Context(t), traceParentID)
	)

	t.RunSynced("ydb.New", func(t *xtest.SyncedTest) {
		db, err := ydb.New(ctx, //nolint:gocritic
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
				newLogger(t),
				trace.MatchDetails(`ydb\.(driver|discovery|retry|scheme).*`),
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
	})
	t.RunSynced("ydb.Open", func(t *xtest.SyncedTest) {
		db, err := ydb.Open(ctx,
			os.Getenv("YDB_CONNECTION_STRING"),
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
		t.RunSynced("WithStaticCredentials", func(t *xtest.SyncedTest) {
			db, err := ydb.Open(ctx,
				os.Getenv("YDB_CONNECTION_STRING"),
				ydb.WithAccessTokenCredentials(
					os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
				),
			)
			require.NoError(t, err)
			defer func() {
				_ = db.Close(ctx)
			}()
			err = db.Query().Exec(ctx, `DROP USER IF EXISTS test`)
			require.NoError(t, err)
			err = db.Query().Exec(ctx, "CREATE USER test PASSWORD 'password'; ALTER GROUP `ADMINS` ADD USER test;")
			require.NoError(t, err)
			defer func() {
				_ = db.Query().Exec(ctx, `DROP USER test`)
			}()
			t.RunSynced("UsingConnectionString", func(t *xtest.SyncedTest) {
				t.RunSynced("HappyWay", func(t *xtest.SyncedTest) {
					u, err := url.Parse(os.Getenv("YDB_CONNECTION_STRING"))
					require.NoError(t, err)
					u.User = url.UserPassword("test", "password")
					t.Log(u.String())
					db, err := ydb.Open(ctx, u.String())
					require.NoError(t, err)
					defer func() {
						_ = db.Close(ctx)
					}()
					row, err := db.Query().QueryRow(ctx, `SELECT 1`)
					require.NoError(t, err)
					var v int
					err = row.Scan(&v)
					require.NoError(t, err)
					tableName := path.Join(db.Name(), t.Name(), "test")
					t.RunSynced("CreateTable", func(t *xtest.SyncedTest) {
						err := db.Query().Exec(ctx, fmt.Sprintf(`
							CREATE TABLE IF NOT EXISTS %s (
								id Uint64,
								value Utf8,
								PRIMARY KEY (id)
							)`, "`"+tableName+"`"),
						)
						require.NoError(t, err)
					})
					t.RunSynced("DescribeTable", func(t *xtest.SyncedTest) {
						var d options.Description
						err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
							d, err = s.DescribeTable(ctx, tableName)
							if err != nil {
								return err
							}

							return nil
						})
						require.NoError(t, err)
						require.Equal(t, "test", d.Name)
						require.Equal(t, 2, len(d.Columns))
						require.Equal(t, "id", d.Columns[0].Name)
						require.Equal(t, "value", d.Columns[1].Name)
						require.Equal(t, []string{"id"}, d.PrimaryKey)
					})
				})
				t.RunSynced("WrongLogin", func(t *xtest.SyncedTest) {
					u, err := url.Parse(os.Getenv("YDB_CONNECTION_STRING"))
					require.NoError(t, err)
					u.User = url.UserPassword("wrong_login", "password")
					db, err := ydb.Open(ctx, u.String())
					require.Error(t, err)
					require.Nil(t, db)
					require.True(t, credentials.IsAccessError(err))
				})
				t.RunSynced("WrongPassword", func(t *xtest.SyncedTest) {
					u, err := url.Parse(os.Getenv("YDB_CONNECTION_STRING"))
					require.NoError(t, err)
					u.User = url.UserPassword("test", "wrong_password")
					db, err := ydb.Open(ctx, u.String())
					require.Error(t, err)
					require.Nil(t, db)
					require.True(t, credentials.IsAccessError(err))
				})
			})
			t.RunSynced("UsingExplicitStaticCredentials", func(t *xtest.SyncedTest) {
				t.RunSynced("HappyWay", func(t *xtest.SyncedTest) {
					t.RunSynced("WithStaticCredentials", func(t *xtest.SyncedTest) {
						db, err := ydb.Open(ctx,
							os.Getenv("YDB_CONNECTION_STRING"),
							ydb.WithStaticCredentials("test", "password"),
						)
						require.NoError(t, err)
						defer func() {
							_ = db.Close(ctx)
						}()
						tableName := path.Join(db.Name(), t.Name(), "test")
						t.RunSynced("CreateTable", func(t *xtest.SyncedTest) {
							err := db.Query().Exec(ctx, fmt.Sprintf(`
								CREATE TABLE IF NOT EXISTS %s (
									id Uint64,
									value Utf8,
									PRIMARY KEY (id)
								)`, "`"+tableName+"`"),
							)
							require.NoError(t, err)
						})
						t.RunSynced("Query", func(t *xtest.SyncedTest) {
							row, err := db.Query().QueryRow(ctx, `SELECT 1`)
							require.NoError(t, err)
							var v int
							err = row.Scan(&v)
							require.NoError(t, err)
						})
						t.RunSynced("DescribeTable", func(t *xtest.SyncedTest) {
							var d options.Description
							err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
								d, err = s.DescribeTable(ctx, tableName)
								if err != nil {
									return err
								}

								return nil
							})
							require.NoError(t, err)
							require.Equal(t, "test", d.Name)
							require.Equal(t, 2, len(d.Columns))
							require.Equal(t, "id", d.Columns[0].Name)
							require.Equal(t, "value", d.Columns[1].Name)
							require.Equal(t, []string{"id"}, d.PrimaryKey)
						})
					})
					t.RunSynced("WithStaticCredentialsLogin+WithStaticCredentialsPassword",
						func(t *xtest.SyncedTest) {
							db, err := ydb.Open(ctx,
								os.Getenv("YDB_CONNECTION_STRING"),
								ydb.WithStaticCredentialsLogin("test"),
								ydb.WithStaticCredentialsPassword("password"),
							)
							require.NoError(t, err)
							defer func() {
								_ = db.Close(ctx)
							}()
							tableName := path.Join(db.Name(), t.Name(), "test")
							t.RunSynced("CreateTable", func(t *xtest.SyncedTest) {
								err := db.Query().Exec(ctx, fmt.Sprintf(`
							CREATE TABLE IF NOT EXISTS %s (
								id Uint64,
								value Utf8,
								PRIMARY KEY (id)
							)`, "`"+tableName+"`"),
								)
								require.NoError(t, err)
							})
							t.RunSynced("Query", func(t *xtest.SyncedTest) {
								row, err := db.Query().QueryRow(ctx, `SELECT 1`)
								require.NoError(t, err)
								var v int
								err = row.Scan(&v)
								require.NoError(t, err)
							})
							t.RunSynced("DescribeTable", func(t *xtest.SyncedTest) {
								var d options.Description
								err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
									d, err = s.DescribeTable(ctx, tableName)
									if err != nil {
										return err
									}

									return nil
								})
								require.NoError(t, err)
								require.Equal(t, "test", d.Name)
								require.Equal(t, 2, len(d.Columns))
								require.Equal(t, "id", d.Columns[0].Name)
								require.Equal(t, "value", d.Columns[1].Name)
								require.Equal(t, []string{"id"}, d.PrimaryKey)
							})
						})
				})
				t.RunSynced("WrongLogin", func(t *xtest.SyncedTest) {
					db, err := ydb.Open(ctx,
						os.Getenv("YDB_CONNECTION_STRING"),
						ydb.WithStaticCredentials("wrong_user", "password"),
					)
					require.Error(t, err)
					require.Nil(t, db)
					require.True(t, credentials.IsAccessError(err))
				})
				t.RunSynced("WrongPassword", func(t *xtest.SyncedTest) {
					db, err := ydb.Open(ctx,
						os.Getenv("YDB_CONNECTION_STRING"),
						ydb.WithStaticCredentials("test", "wrong_password"),
					)
					require.Error(t, err)
					require.Nil(t, db)
					require.True(t, credentials.IsAccessError(err))
				})
			})
		})
		t.RunSynced("With", func(t *xtest.SyncedTest) {
			t.Run("WithSharedBalancer", func(t *testing.T) {
				child, err := db.With(ctx, ydb.WithSharedBalancer(db))
				require.NoError(t, err)
				result, err := child.Scripting().Execute(ctx, `SELECT 1`, nil)
				require.NoError(t, err)
				require.NoError(t, result.NextResultSetErr(ctx))
				require.True(t, result.NextRow())
				var value int32
				err = result.Scan(indexed.Required(&value))
				require.NoError(t, err)
				require.EqualValues(t, 1, value)
				err = child.Close(ctx)
				require.NoError(t, err)
			})
		})
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

func TestMultipleClosingDriverIssue1585(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	require.NoError(t, err)

	require.NotPanics(t, func() {
		err = db.Close(ctx)
		require.NoError(t, err)

		err = db.Close(ctx)
		require.NoError(t, err)

		err = db.Close(ctx)
		require.NoError(t, err)
	})
}
