package entry

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
)

// Entry represents inserted into the cluster connection.
type Entry struct {
	Conn   conn.Conn
	Handle balancer.Element
	//TrackerQueueEl *list.Element

	Info info.Info
}

func (c *Entry) InsertInto(b balancer.Balancer) {
	if c.Handle != nil {
		panic("ydb: Handle already exists")
	}
	if c.Conn == nil {
		panic("ydb: can't insert nil Conn into balancer")
	}
	c.Handle = b.Insert(c.Conn, c.Info)
	if c.Handle == nil {
		panic("ydb: balancer has returned nil Handle")
	}
}

func (c *Entry) RemoveFrom(b balancer.Balancer) {
	if c.Handle == nil {
		panic("ydb: no Handle to remove from balancer")
	}
	b.Remove(c.Handle)
	c.Handle = nil
}
