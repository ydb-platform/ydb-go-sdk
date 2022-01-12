package proxy

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type proxyScheme struct {
	client scheme.Client
	meta   meta.Meta
}

func Scheme(client scheme.Client, meta meta.Meta) scheme.Client {
	return &proxyScheme{
		client: client,
		meta:   meta,
	}
}

func (s *proxyScheme) ModifyPermissions(
	ctx context.Context,
	path string,
	opts ...scheme.PermissionsOption,
) (err error) {
	ctx, err = s.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return s.client.ModifyPermissions(ctx, path, opts...)
}

func (s *proxyScheme) Close(ctx context.Context) (err error) {
	ctx, err = s.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return s.client.Close(ctx)
}

func (s *proxyScheme) DescribePath(ctx context.Context, path string) (e scheme.Entry, err error) {
	ctx, err = s.meta.Meta(ctx)
	if err != nil {
		return e, err
	}
	return s.client.DescribePath(ctx, path)
}

func (s *proxyScheme) MakeDirectory(ctx context.Context, path string) (err error) {
	ctx, err = s.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return s.client.MakeDirectory(ctx, path)
}

func (s *proxyScheme) ListDirectory(ctx context.Context, path string) (d scheme.Directory, err error) {
	ctx, err = s.meta.Meta(ctx)
	if err != nil {
		return d, err
	}
	return s.client.ListDirectory(ctx, path)
}

func (s *proxyScheme) RemoveDirectory(ctx context.Context, path string) (err error) {
	ctx, err = s.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return s.client.RemoveDirectory(ctx, path)
}
