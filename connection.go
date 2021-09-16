package ydb

import (
	"context"
	"fmt"
	"path"
	"strings"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
)

type Connection struct {
	database string
	options  options
	cluster  conn.Cluster
	table    *tableWrapper
	scheme   *schemeWrapper
}

func (c *Connection) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	return c.cluster.Invoke(ctx, method, args, reply, opts...)
}

func (c *Connection) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.cluster.NewStream(ctx, desc, method, opts...)
}

func (c *Connection) Stats(it func(endpoint.Endpoint, stats.Stats)) {
	c.cluster.Stats(it)
}

func (c *Connection) Close() error {
	_ = c.table.Pool().Close(context.Background())
	return c.cluster.Close()
}

func (c *Connection) Table() *tableWrapper {
	return c.table
}

func (c *Connection) Scheme() *scheme.Client {
	return c.scheme.client
}

func (c *Connection) EnsurePathExists(ctx context.Context, path string) error {
	for i := len(c.database); i < len(path); i++ {
		x := strings.IndexByte(path[i:], '/')
		if x == -1 {
			x = len(path[i:]) - 1
		}
		i += x
		sub := path[:i+1]
		info, err := c.Scheme().DescribePath(ctx, sub)
		operr, ok := err.(*errors.OpError)
		if ok && operr.Reason == errors.StatusSchemeError {
			err = c.Scheme().MakeDirectory(ctx, sub)
		}
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		switch info.Type {
		case
			scheme.EntryDatabase,
			scheme.EntryDirectory:
			// OK
		default:
			return fmt.Errorf(
				"entry %q exists but it is a %s",
				sub, info.Type,
			)
		}
	}

	return nil
}

func (c *Connection) CleanupDatabase(ctx context.Context, prefix string, names ...string) error {
	filter := make(map[string]struct{}, len(names))
	for _, n := range names {
		filter[n] = struct{}{}
	}
	var list func(int, string) error
	list = func(i int, p string) error {
		dir, err := c.Scheme().ListDirectory(ctx, p)
		operr, ok := err.(*errors.OpError)
		if ok && operr.Reason == errors.StatusSchemeError {
			return nil
		}
		if err != nil {
			return err
		}
		for _, child := range dir.Children {
			if _, has := filter[child.Name]; !has {
				continue
			}
			pt := path.Join(p, child.Name)
			switch child.Type {
			case scheme.EntryDirectory:
				if err := list(i+1, pt); err != nil {
					return err
				}
				if err := c.Scheme().RemoveDirectory(ctx, pt); err != nil {
					return err
				}

			case scheme.EntryTable:
				s, err := c.Table().Pool().Get(ctx)
				if err != nil {
					return err
				}
				err = s.DropTable(ctx, pt)
				_ = c.Table().Pool().Put(ctx, s)
				if err != nil {
					return err
				}

			default:

			}
		}
		return nil
	}
	return list(0, prefix)
}
