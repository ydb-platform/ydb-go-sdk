package connect

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
)

type tableWrapper struct {
	client      *table.Client
	sessionPool *table.SessionPool
}

func (t *tableWrapper) set(cluster conn.Cluster, o options) {
	{
		var opts []table.ClientOption
		if o.tableClientTrace != nil {
			opts = append(opts, table.WithClientTraceOption(*o.tableClientTrace))
		}
		t.client = table.NewClient(cluster, opts...)
	}
	{
		t.sessionPool = &table.SessionPool{}
		if o.tableSessionPoolTrace != nil {
			t.sessionPool.Trace = *o.tableSessionPoolTrace
		}
		if o.tableSessionPoolSizeLimit != nil {
			t.sessionPool.SizeLimit = *o.tableSessionPoolSizeLimit
		}
		if o.tableSessionPoolKeepAliveMinSize != nil {
			t.sessionPool.KeepAliveMinSize = *o.tableSessionPoolKeepAliveMinSize
		}
		if o.tableSessionPoolIdleThreshold != nil {
			t.sessionPool.IdleThreshold = *o.tableSessionPoolIdleThreshold
		}
		if o.tableSessionPoolKeepAliveTimeout != nil {
			t.sessionPool.KeepAliveTimeout = *o.tableSessionPoolKeepAliveTimeout
		}
		if o.tableSessionPoolCreateSessionTimeout != nil {
			t.sessionPool.CreateSessionTimeout = *o.tableSessionPoolCreateSessionTimeout
		}
		if o.tableSessionPoolDeleteTimeout != nil {
			t.sessionPool.DeleteTimeout = *o.tableSessionPoolDeleteTimeout
		}
	}
	t.sessionPool.Builder = t.client
}

func (t *tableWrapper) CreateSession(ctx context.Context) (*table.Session, error) {
	return t.client.CreateSession(ctx)
}

func (t *tableWrapper) Pool() *table.SessionPool {
	return t.sessionPool
}
