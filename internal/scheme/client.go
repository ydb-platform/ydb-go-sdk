package scheme

import (
	"context"
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

//nolint:gofumpt
//nolint:nolintlint
var (
	errNilClient = xerrors.Wrap(errors.New("scheme client is not initialized"))
)

type Client struct {
	config  config.Config
	service Ydb_Scheme_V1.SchemeServiceClient
}

func (c *Client) Close(_ context.Context) error {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
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
	call := func(ctx context.Context) error {
		return xerrors.WithStackTrace(c.makeDirectory(ctx, path))
	}
	if !c.config.AutoRetry() {
		return call(ctx)
	}
	return retry.Retry(ctx, call, retry.WithStackTrace(), retry.WithIdempotent(true))
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
	call := func(ctx context.Context) error {
		return xerrors.WithStackTrace(c.removeDirectory(ctx, path))
	}
	if !c.config.AutoRetry() {
		return call(ctx)
	}
	return retry.Retry(ctx, call, retry.WithStackTrace(), retry.WithIdempotent(true))
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
	call := func(ctx context.Context) error {
		d, err = c.listDirectory(ctx, path)
		return xerrors.WithStackTrace(err)
	}
	if !c.config.AutoRetry() {
		err = call(ctx)
		return
	}
	err = retry.Retry(ctx, call, retry.WithIdempotent(true), retry.WithStackTrace())
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
	call := func(ctx context.Context) error {
		e, err = c.describePath(ctx, path)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		return nil
	}
	if !c.config.AutoRetry() {
		err = call(ctx)
		return
	}
	err = retry.Retry(ctx, call, retry.WithIdempotent(true), retry.WithStackTrace())
	if err != nil {
		return e, xerrors.WithStackTrace(err)
	}
	return e, nil
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
	call := func(ctx context.Context) error {
		return xerrors.WithStackTrace(c.modifyPermissions(ctx, path, opts...))
	}
	if !c.config.AutoRetry() {
		return call(ctx)
	}
	return retry.Retry(ctx, call, retry.WithStackTrace(), retry.WithIdempotent(true))
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
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}

func putEntry(dst []scheme.Entry, src []*Ydb_Scheme.Entry) {
	for i, e := range src {
		(dst[i]).From(e)
	}
}
