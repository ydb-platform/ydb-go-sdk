package scheme

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

// nolint: gofumpt
// nolint: nolintlint
var (
	errNilClient = xerrors.Wrap(errors.New("scheme client is not initialized"))
)

type Client struct {
	config  config.Config
	service Ydb_Scheme_V1.SchemeServiceClient
}

func (c *Client) Close(_ context.Context) error {
	return nil
}

func New(cc grpc.ClientConnInterface, config config.Config) *Client {
	return &Client{
		config:  config,
		service: Ydb_Scheme_V1.NewSchemeServiceClient(cc),
	}
}

func (c *Client) MakeDirectory(ctx context.Context, path string) (err error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	if !c.config.AutoRetry() {
		return xerrors.WithStackTrace(c.makeDirectory(ctx, path))
	}
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return xerrors.WithStackTrace(c.makeDirectory(ctx, path))
	}, retry.WithStackTrace())
}

func (c *Client) makeDirectory(ctx context.Context, path string) (err error) {
	_, err = c.service.MakeDirectory(
		ctx,
		&Ydb_Scheme.MakeDirectoryRequest{
			Path: path,
			OperationParams: operation.Params(
				ctx,
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	return xerrors.WithStackTrace(err)
}

func (c *Client) RemoveDirectory(ctx context.Context, path string) (err error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	if !c.config.AutoRetry() {
		return xerrors.WithStackTrace(c.removeDirectory(ctx, path))
	}
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return xerrors.WithStackTrace(c.removeDirectory(ctx, path))
	}, retry.WithStackTrace())
}

func (c *Client) removeDirectory(ctx context.Context, path string) (err error) {
	_, err = c.service.RemoveDirectory(
		ctx,
		&Ydb_Scheme.RemoveDirectoryRequest{
			Path: path,
			OperationParams: operation.Params(
				ctx,
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	return xerrors.WithStackTrace(err)
}

func (c *Client) ListDirectory(ctx context.Context, path string) (d scheme.Directory, err error) {
	if c == nil {
		return d, xerrors.WithStackTrace(errNilClient)
	}
	if !c.config.AutoRetry() {
		d, err = c.listDirectory(ctx, path)
		return d, xerrors.WithStackTrace(err)
	}
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		d, err = c.listDirectory(ctx, path)
		return xerrors.WithStackTrace(err)
	}, retry.WithIdempotent(true), retry.WithStackTrace())
	return
}

func (c *Client) listDirectory(ctx context.Context, path string) (scheme.Directory, error) {
	var (
		d        scheme.Directory
		err      error
		response *Ydb_Scheme.ListDirectoryResponse
		result   Ydb_Scheme.ListDirectoryResult
	)
	response, err = c.service.ListDirectory(
		ctx,
		&Ydb_Scheme.ListDirectoryRequest{
			Path: path,
			OperationParams: operation.Params(
				ctx,
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	if err != nil {
		return d, xerrors.WithStackTrace(err)
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return d, xerrors.WithStackTrace(err)
	}
	d.From(result.Self)
	d.Children = make([]scheme.Entry, len(result.Children))
	putEntry(d.Children, result.Children)
	return d, nil
}

func (c *Client) DescribePath(ctx context.Context, path string) (e scheme.Entry, err error) {
	if c == nil {
		return e, xerrors.WithStackTrace(errNilClient)
	}
	if !c.config.AutoRetry() {
		e, err = c.describePath(ctx, path)
		return e, xerrors.WithStackTrace(err)
	}
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		e, err = c.describePath(ctx, path)
		return xerrors.WithStackTrace(err)
	}, retry.WithIdempotent(true), retry.WithStackTrace())
	return
}

func (c *Client) describePath(ctx context.Context, path string) (e scheme.Entry, err error) {
	var (
		response *Ydb_Scheme.DescribePathResponse
		result   Ydb_Scheme.DescribePathResult
	)
	response, err = c.service.DescribePath(
		ctx,
		&Ydb_Scheme.DescribePathRequest{
			Path: path,
			OperationParams: operation.Params(
				ctx,
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	if err != nil {
		return e, xerrors.WithStackTrace(err)
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return e, xerrors.WithStackTrace(err)
	}
	e.From(result.Self)
	return e, nil
}

func (c *Client) ModifyPermissions(ctx context.Context, path string, opts ...scheme.PermissionsOption) (err error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	if !c.config.AutoRetry() {
		return xerrors.WithStackTrace(c.modifyPermissions(ctx, path, opts...))
	}
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return xerrors.WithStackTrace(c.modifyPermissions(ctx, path, opts...))
	}, retry.WithStackTrace())
}

func (c *Client) modifyPermissions(ctx context.Context, path string, opts ...scheme.PermissionsOption) (err error) {
	var desc permissionsDesc
	for _, o := range opts {
		o(&desc)
	}
	_, err = c.service.ModifyPermissions(
		ctx,
		&Ydb_Scheme.ModifyPermissionsRequest{
			Path:             path,
			Actions:          desc.actions,
			ClearPermissions: desc.clear,
			OperationParams: operation.Params(
				ctx,
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	return xerrors.WithStackTrace(err)
}

func putEntry(dst []scheme.Entry, src []*Ydb_Scheme.Entry) {
	for i, e := range src {
		(dst[i]).From(e)
	}
}
