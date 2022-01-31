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

func (c *Entry) InsertInto(b balancer.Balancer) {
	if c.Handle != nil {
		panic("ydb: Handle already exists")
	}
	if c.Conn == nil {
		panic("ydb: can't insert nil Conn into balancer")
	}
	c.Handle = b.Insert(c.Conn)
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
