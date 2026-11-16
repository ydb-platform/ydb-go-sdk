package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
)

func TestTableSessionExecuteOverQueryKeepsAlive(t *testing.T) {
	for _, code := range []codes.Code{codes.ResourceExhausted, codes.OutOfRange} {
		t.Run(code.String(), func(t *testing.T) {
			mockSrv := mock.Server(t)
			ctx := t.Context()
			driver, err := ydb.Open(ctx, mockSrv.ConnString(),
				ydb.WithAnonymousCredentials(),
				ydb.WithExecuteDataQueryOverQueryClient(true),
				ydb.With(config.WithGrpcOptions(grpc.WithChainStreamInterceptor(
					func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
						method string, streamer grpc.Streamer, opts ...grpc.CallOption,
					) (grpc.ClientStream, error) {
						if method == Ydb_Query_V1.QueryService_ExecuteQuery_FullMethodName {
							return nil, status.Error(code, "query failed")
						}

						return streamer(ctx, desc, cc, method, opts...)
					},
				))),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, driver.Close(ctx)) }()

			err = driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
				_, _, err := s.Execute(ctx, table.DefaultTxControl(), "SELECT 42", nil)
				require.True(t, ydb.IsTransportError(err, code))
				require.Equal(t, table.SessionReady, s.Status())

				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestTableSessionExecuteWithExecuteDataQueryOverQueryClient verifies
// that the ydb.WithExecuteDataQueryOverQueryClient option correctly switches the
// underlying gRPC method that backs table.Session.Execute:
//
//   - WithExecuteDataQueryOverQueryClient(true)  → Ydb_Query_V1.QueryService.ExecuteQuery
//     (queryClientExecutor.execute in internal/table/session.go)
//   - WithExecuteDataQueryOverQueryClient(false) → Ydb_Table_V1.TableService.ExecuteDataQuery
//     (tableClientExecutor.execute in internal/table/session.go)
//
// The test uses the in-process mock server's per-method call counters to assert
// the exact dispatch path. The result payload is identical for both paths (the
// mock interprets `SELECT 42` the same way over either service), so the test
// also verifies that the materialized `result.Result` is observably the same to
// the caller regardless of which executor handled the query.
func TestTableSessionExecuteDataQuery(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		overQueryClient           bool
		wantExecuteQueryCalls     uint64
		wantExecuteDataQueryCalls uint64
	}{
		{
			// queryClientExecutor: Execute is forwarded to the streaming
			// QueryService.ExecuteQuery RPC, the legacy unary
			// TableService.ExecuteDataQuery is never reached.
			name:                      "over query-client",
			overQueryClient:           true,
			wantExecuteQueryCalls:     1,
			wantExecuteDataQueryCalls: 0,
		},
		{
			// tableClientExecutor (default): Execute calls the legacy unary
			// TableService.ExecuteDataQuery RPC; QueryService.ExecuteQuery is
			// not reached.
			name:                      "original table-client",
			overQueryClient:           false,
			wantExecuteQueryCalls:     0,
			wantExecuteDataQueryCalls: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockSrv := mock.Server(t)

			ctx := context.Background()

			driver, err := ydb.Open(ctx, mockSrv.ConnString(),
				ydb.WithAnonymousCredentials(),
				ydb.WithExecuteDataQueryOverQueryClient(tc.overQueryClient),
			)
			require.NoError(t, err)

			t.Cleanup(func() {
				_ = driver.Close(ctx)
			})

			err = driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
				_, res, err := s.Execute(ctx, table.DefaultTxControl(), `SELECT 42`, nil)
				if err != nil {
					return err
				}
				defer func() {
					_ = res.Close()
				}()

				if err = res.NextResultSetErr(ctx); err != nil {
					return err
				}

				require.True(t, res.NextRow())

				var v int32
				require.NoError(t, res.Scan(indexed.Required(&v)))
				require.Equal(t, int32(42), v)

				return res.Err()
			}, table.WithIdempotent())
			require.NoError(t, err)

			require.Equal(t, tc.wantExecuteQueryCalls, mockSrv.ExecuteQueryCalls(),
				"unexpected number of QueryService.ExecuteQuery invocations")
			require.Equal(t, tc.wantExecuteDataQueryCalls, mockSrv.ExecuteDataQueryCalls(),
				"unexpected number of TableService.ExecuteDataQuery invocations")
		})
	}
}
