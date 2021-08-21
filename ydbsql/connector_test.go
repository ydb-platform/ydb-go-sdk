package ydbsql

import (
	"context"
	"database/sql"
	"errors"
	ydb "github.com/YandexDatabase/ydb-go-sdk/v2"
	"github.com/YandexDatabase/ydb-go-sdk/v2/testutil"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"

	"github.com/YandexDatabase/ydb-go-sdk/v2/table"
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
			DriverConfig: &ydb.DriverConfig{},
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
		cacheSize              int
		prepareCount           int
		prepareRequestsCount   int
		queryCachePolicyOption []table.QueryCachePolicyOption
	}{
		{
			name:                   "fixed query cache size, with server cache, one request proxed to server",
			cacheSize:              10,
			prepareCount:           10,
			prepareRequestsCount:   1,
			queryCachePolicyOption: []table.QueryCachePolicyOption{table.WithQueryCachePolicyKeepInCache()},
		},
		{
			name:                   "default query cache size, with server cache, one request proxed to server",
			cacheSize:              0,
			prepareCount:           10,
			prepareRequestsCount:   1,
			queryCachePolicyOption: []table.QueryCachePolicyOption{table.WithQueryCachePolicyKeepInCache()},
		},
		{
			name:                   "disabled query cache, with server cache, all requests proxed to server",
			cacheSize:              -1,
			prepareCount:           10,
			prepareRequestsCount:   10,
			queryCachePolicyOption: []table.QueryCachePolicyOption{table.WithQueryCachePolicyKeepInCache()},
		},
		{
			name:                   "fixed query cache size, no server cache, one request proxed to server",
			cacheSize:              10,
			prepareCount:           10,
			prepareRequestsCount:   1,
			queryCachePolicyOption: []table.QueryCachePolicyOption{},
		},
		{
			name:                   "default query cache size, no server cache, one request proxed to server",
			cacheSize:              0,
			prepareCount:           10,
			prepareRequestsCount:   1,
			queryCachePolicyOption: []table.QueryCachePolicyOption{},
		},
		{
			name:                   "disabled query cache, no server cache, all requests proxed to server",
			cacheSize:              -1,
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
				WithClient(&table.Client{
					Driver: &testutil.Driver{
						OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface {
						}) error {
							switch m {
							case testutil.TableCreateSession:
							case testutil.TableExecuteDataQuery:
								r := testutil.TableExecuteDataQueryRequest{req}
								if len(test.queryCachePolicyOption) > 0 {
									keepInCache, ok := r.KeepInCache()
									require.True(t, ok)
									require.True(t, keepInCache)
								} else {
									keepInCache, ok := r.KeepInCache()
									require.True(t, ok)
									require.False(t, keepInCache)
								}
								{
									r := testutil.TableExecuteDataQueryResult{res}
									r.SetTransactionID("")
								}
								return nil
							default:
								t.Fatalf("Unexpected method %d", m)
							}
							return nil
						},
					},
					MaxQueryCacheSize: test.cacheSize,
				}),
				WithDefaultExecDataQueryOption(table.WithQueryCachePolicy(test.queryCachePolicyOption...)),
			)
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			rows, err := db.QueryContext(ctx, "SELECT 1")
			require.NoError(t, err)
			require.NotNil(t, rows)
		})
	}
}
