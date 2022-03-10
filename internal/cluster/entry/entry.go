package entry

import (
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

// Entry represents inserted into the cluster connection.
type Entry struct {
	Conn   conn.Conn
	Handle balancer.Element
}

func (c *Entry) InsertInto(b balancer.Balancer, mtx *sync.RWMutex) bool {
	if c.Handle != nil {
		panic("ydb: Handle already exists")
	}
	if c.Conn == nil {
		panic("ydb: can't insert nil Conn into balancer")
	}
	mtx.Lock()
	c.Handle = b.Insert(c.Conn)
	mtx.Unlock()
	return c.Handle != nil
}

func (c *Entry) RemoveFrom(b balancer.Balancer, mtx *sync.RWMutex) bool {
	if c.Handle == nil {
		return false
	}
	mtx.Lock()
	b.Remove(c.Handle)
	mtx.Unlock()
	c.Handle = nil
	return true
}
