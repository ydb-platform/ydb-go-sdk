package ydb

import (
	context "context"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
	"sync"
)

type lazyScheme struct {
	cluster cluster.Cluster
	client  scheme.Client
	once    sync.Once
}

func (s *lazyScheme) Close(ctx context.Context) error {
	s.init()
	return s.client.Close(ctx)
}

func (s *lazyScheme) init() {
	s.once.Do(func() {
		s.client = scheme.New(s.cluster)
	})
}

func (s *lazyScheme) CleanupDatabase(ctx context.Context, prefix string, names ...string) error {
	s.init()
	return s.client.CleanupDatabase(ctx, prefix, names...)
}

func (s *lazyScheme) EnsurePathExists(ctx context.Context, path string) error {
	s.init()
	return s.client.EnsurePathExists(ctx, path)
}

func (s *lazyScheme) DescribePath(ctx context.Context, path string) (e scheme.Entry, err error) {
	s.init()
	return s.client.DescribePath(ctx, path)
}

func (s *lazyScheme) MakeDirectory(ctx context.Context, path string) (err error) {
	s.init()
	return s.client.MakeDirectory(ctx, path)
}

func (s *lazyScheme) ListDirectory(ctx context.Context, path string) (d scheme.Directory, err error) {
	s.init()
	return s.client.ListDirectory(ctx, path)
}

func (s *lazyScheme) RemoveDirectory(ctx context.Context, path string) (err error) {
	s.init()
	return s.client.RemoveDirectory(ctx, path)
}

func newScheme(cluster cluster.Cluster) *lazyScheme {
	return &lazyScheme{
		cluster: cluster,
	}
}
