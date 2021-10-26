package ydb

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type dbWithTable interface {
	DB

	Table() table.Client
}

type lazyScheme struct {
	db     dbWithTable
	client scheme.Scheme
	m      sync.Mutex
}

func (s *lazyScheme) ModifyPermissions(ctx context.Context, path string, opts ...scheme.PermissionsOption) (err error) {
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
		s.client = internal.New(s.db)
	}
	s.m.Unlock()
}

func (s *lazyScheme) EnsurePathExists(ctx context.Context, path string) error {
	for i := len(s.db.Name()); i < len(path); i++ {
		x := strings.IndexByte(path[i:], '/')
		if x == -1 {
			x = len(path[i:]) - 1
		}
		i += x
		sub := path[:i+1]
		info, err := s.DescribePath(ctx, sub)
		operr, ok := err.(*errors.OpError)
		if ok && operr.Reason == errors.StatusSchemeError {
			err = s.MakeDirectory(ctx, sub)
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

func (s *lazyScheme) CleanupDatabase(ctx context.Context, prefix string, names ...string) error {
	filter := make(map[string]struct{}, len(names))
	for _, n := range names {
		filter[n] = struct{}{}
	}
	var list func(int, string) error
	list = func(i int, p string) error {
		dir, err := s.ListDirectory(ctx, p)
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
				if err = list(i+1, pt); err != nil {
					return err
				}
				if err = s.RemoveDirectory(ctx, pt); err != nil {
					return err
				}

			case scheme.EntryTable:
				err = s.db.Table().Do(ctx, func(ctx context.Context, session table.Session) (err error) {
					return session.DropTable(ctx, pt)
				})
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
