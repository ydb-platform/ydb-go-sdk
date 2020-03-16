package ydbsql

import (
	"context"
	"database/sql"
	"errors"
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
		}),
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
