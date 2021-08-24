package ydb

import (
	"google.golang.org/grpc"
	"time"

	"github.com/YandexDatabase/ydb-go-sdk/v2/internal/stats"
)

type conn struct {
	raw  *grpc.ClientConn
	addr connAddr

	runtime connRuntime
}

func (c *conn) Address() string {
	if c != nil {
		return c.addr.String()
	}
	return ""
}

func newConn(cc *grpc.ClientConn, addr connAddr) *conn {
	const (
		statsDuration = time.Minute
		statsBuckets  = 12
	)
	return &conn{
		raw:  cc,
		addr: addr,
		runtime: connRuntime{
			opTime:  stats.NewSeries(statsDuration, statsBuckets),
			opRate:  stats.NewSeries(statsDuration, statsBuckets),
			errRate: stats.NewSeries(statsDuration, statsBuckets),
		},
	}
}
