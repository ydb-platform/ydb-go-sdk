package ydb

import (
	"google.golang.org/grpc"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/v2/internal/stats"
)

type conn struct {
	conn *grpc.ClientConn
	addr connAddr

	runtime connRuntime
}

func (conn *conn) Conn() *conn {
	return conn
}

func (conn *conn) Address() string {
	if conn != nil {
		return conn.addr.String()
	}
	return ""
}

func newConn(cc *grpc.ClientConn, addr connAddr) *conn {
	const (
		statsDuration = time.Minute
		statsBuckets  = 12
	)
	return &conn{
		conn: cc,
		addr: addr,
		runtime: connRuntime{
			opTime:  stats.NewSeries(statsDuration, statsBuckets),
			opRate:  stats.NewSeries(statsDuration, statsBuckets),
			errRate: stats.NewSeries(statsDuration, statsBuckets),
		},
	}
}
