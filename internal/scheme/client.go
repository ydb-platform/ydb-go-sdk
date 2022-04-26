package scheme

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
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

func (c *Client) ListDirectory(ctx context.Context, path string) (scheme.Directory, error) {
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
