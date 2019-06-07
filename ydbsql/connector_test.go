package ydbsql

import (
	"context"
	"database/sql"
	"net"
	"testing"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
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
