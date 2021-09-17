package ydb

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type lazyTable struct {
	client      table.Client
	sessionPool *table.SessionPool
}

func (t *lazyTable) set(cluster cluster.Cluster, o options) {
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

func (t *lazyTable) CreateSession(ctx context.Context) (*table.Session, error) {
	return t.client.CreateSession(ctx)
}

func (t *lazyTable) Pool() *table.SessionPool {
	return t.sessionPool
}
