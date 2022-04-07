package entry

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

// Entry represents inserted into the cluster connection.
type Entry struct {
	Conn   conn.Conn
	Handle balancer.Element
}
