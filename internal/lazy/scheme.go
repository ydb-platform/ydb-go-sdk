package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/db"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type lazyScheme struct {
	db     db.Connection
	client ydb_scheme.Client
	m      sync.Mutex
}

func Scheme(db db.Connection) ydb_scheme.Client {
	return &lazyScheme{
		db: db,
	}
}

func (s *lazyScheme) ModifyPermissions(
	ctx context.Context,
	path string,
	opts ...ydb_scheme.PermissionsOption,
) (err error) {
	s.init()
	return s.client.ModifyPermissions(ctx, path, opts...)
}

func (s *lazyScheme) Close(ctx context.Context) error {
	s.m.Lock()
	defer s.m.Unlock()
	if s.client == nil {
		return nil
	}
	defer func() {
		s.client = nil
	}()
	return s.client.Close(ctx)
}

func (s *lazyScheme) init() {
	s.m.Lock()
	if s.client == nil {
		s.client = scheme.New(s.db)
	}
	s.m.Unlock()
}

func (s *lazyScheme) DescribePath(ctx context.Context, path string) (e ydb_scheme.Entry, err error) {
	s.init()
	return s.client.DescribePath(ctx, path)
}

func (s *lazyScheme) MakeDirectory(ctx context.Context, path string) (err error) {
	s.init()
	return s.client.MakeDirectory(ctx, path)
}

func (s *lazyScheme) ListDirectory(ctx context.Context, path string) (d ydb_scheme.Directory, err error) {
	s.init()
	return s.client.ListDirectory(ctx, path)
}

func (s *lazyScheme) RemoveDirectory(ctx context.Context, path string) (err error) {
	s.init()
	return s.client.RemoveDirectory(ctx, path)
}
