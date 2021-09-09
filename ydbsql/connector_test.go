package ydbsql

import (
	"context"
	"database/sql"
	"errors"
	"google.golang.org/protobuf/proto"
	"net"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestConnectorDialOnPing(t *testing.T) {
	const timeout = time.Second

	client, server := net.Pipe()
	defer server.Close()

	dial := make(chan struct{})
	c := Connector(
		WithEndpoint("127.0.0.1:9999"),
		WithDialer(ydb.Dialer{
			NetDial: func(_ context.Context, addr string) (net.Conn, error) {
				dial <- struct{}{}
				return client, nil
			},
			DriverConfig: &ydb.DriverConfig{
				Credentials: ydb.NewAnonymousCredentials("test"),
			},
		}),
	)

	db := sql.OpenDB(c)
	select {
	case <-dial:
		t.Fatalf("unexpected dial")
	case <-time.After(timeout):
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go db.PingContext(ctx)

	select {
	case <-dial:
	case <-time.After(timeout):
		t.Fatalf("no dial after %s", timeout)
	}
}

// KIKIMR-8592: check that we try re-dial after any error
func TestConnectorRedialOnError(t *testing.T) {
	const timeout = 100 * time.Millisecond

	client, server := net.Pipe()
	defer server.Close()
	success := make(chan bool, 1)

	dial := false
	c := Connector(
		WithEndpoint("127.0.0.1:9999"),
		WithDialer(ydb.Dialer{
			NetDial: func(_ context.Context, addr string) (net.Conn, error) {
				dial = true
				select {
				case <-success:
					// it will still fails on grpc dial
					return client, nil
				default:
					return nil, errors.New("any error")
				}
			},
			DriverConfig: &ydb.DriverConfig{
				Credentials: ydb.NewAnonymousCredentials("test"),
			},
		}),
		WithDefaultTxControl(table.TxControl(
			table.BeginTx(
				table.WithStaleReadOnly(),
			),
			table.CommitTx()),
		),
	)

	db := sql.OpenDB(c)
	for i := 0; i < 3; i++ {
		success <- i%2 == 0
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		_ = db.PingContext(ctx)
		if !dial {
			t.Fatalf("no dial on re-ping at %v iteration", i)
		}
		dial = false
	}
}

func TestConnectorWithQueryCachePolicyKeepInCache(t *testing.T) {
	for _, test := range [...]struct {
		name                   string
		prepareCount           int
		prepareRequestsCount   int
		queryCachePolicyOption []table.QueryCachePolicyOption
	}{
		{
			name:                   "with server cache, one request proxed to server",
			prepareCount:           10,
			prepareRequestsCount:   1,
			queryCachePolicyOption: []table.QueryCachePolicyOption{table.WithQueryCachePolicyKeepInCache()},
		},
		{
			name:                   "with server cache, all requests proxed to server",
			prepareCount:           10,
			prepareRequestsCount:   10,
			queryCachePolicyOption: []table.QueryCachePolicyOption{table.WithQueryCachePolicyKeepInCache()},
		},
		{
			name:                   "no server cache, one request proxed to server",
			prepareCount:           10,
			prepareRequestsCount:   1,
			queryCachePolicyOption: []table.QueryCachePolicyOption{},
		},
		{
			name:                   "no server cache, all requests proxed to server",
			prepareCount:           10,
			prepareRequestsCount:   10,
			queryCachePolicyOption: []table.QueryCachePolicyOption{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()

			c := Connector(
				WithClient(
					table.NewClient(
						testutil.NewCluster(
							testutil.WithInvokeHandlers(
								testutil.InvokeHandlers{
									testutil.TableCreateSession: func(request interface{}) (result proto.Message, err error) {
										return &Ydb_Table.CreateSessionResult{}, nil
									},
									testutil.TableExecuteDataQuery: func(request interface{}) (result proto.Message, err error) {
										r := request.(*Ydb_Table.ExecuteDataQueryRequest)
										keepInCache := r.QueryCachePolicy.KeepInCache
										internal.Equal(t, len(test.queryCachePolicyOption) > 0, keepInCache)
										return &Ydb_Table.ExecuteQueryResult{}, nil
									},
								},
							),
						),
					),
				),
				WithDefaultExecDataQueryOption(table.WithQueryCachePolicy(test.queryCachePolicyOption...)),
			)
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			rows, err := db.QueryContext(ctx, "SELECT 1")
			internal.NoError(t, err)
			internal.NotNil(t, rows)
		})
	}
}
