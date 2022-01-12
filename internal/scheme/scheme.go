package scheme

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type client struct {
	service Ydb_Scheme_V1.SchemeServiceClient
}

func (c *client) Close(_ context.Context) error {
	return nil
}

func New(cc grpc.ClientConnInterface) scheme.Client {
	return &client{
		service: Ydb_Scheme_V1.NewSchemeServiceClient(cc),
	}
}

func (c *client) MakeDirectory(ctx context.Context, path string) (err error) {
	_, err = c.service.MakeDirectory(ctx, &Ydb_Scheme.MakeDirectoryRequest{
		Path: path,
	})
	return err
}

func (c *client) RemoveDirectory(ctx context.Context, path string) (err error) {
	_, err = c.service.RemoveDirectory(ctx, &Ydb_Scheme.RemoveDirectoryRequest{
		Path: path,
	})
	return err
}

func (c *client) ListDirectory(ctx context.Context, path string) (scheme.Directory, error) {
	var (
		d        scheme.Directory
		err      error
		response *Ydb_Scheme.ListDirectoryResponse
		result   Ydb_Scheme.ListDirectoryResult
	)
	response, err = c.service.ListDirectory(ctx, &Ydb_Scheme.ListDirectoryRequest{
		Path: path,
	})
	if err != nil {
		return d, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return d, err
	}
	d.From(result.Self)
	d.Children = make([]scheme.Entry, len(result.Children))
	putEntry(d.Children, result.Children)
	return d, nil
}

func (c *client) DescribePath(ctx context.Context, path string) (e scheme.Entry, err error) {
	var (
		response *Ydb_Scheme.DescribePathResponse
		result   Ydb_Scheme.DescribePathResult
	)
	response, err = c.service.DescribePath(ctx, &Ydb_Scheme.DescribePathRequest{
		Path: path,
	})
	if err != nil {
		return e, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return e, err
	}
	e.From(result.Self)
	return e, nil
}

func (c *client) ModifyPermissions(ctx context.Context, path string, opts ...scheme.PermissionsOption) (err error) {
	var desc permissionsDesc
	for _, o := range opts {
		o(&desc)
	}
	_, err = c.service.ModifyPermissions(ctx, &Ydb_Scheme.ModifyPermissionsRequest{
		Path:             path,
		Actions:          desc.actions,
		ClearPermissions: desc.clear,
	})
	return err
}

func putEntry(dst []scheme.Entry, src []*Ydb_Scheme.Entry) {
	for i, e := range src {
		(dst[i]).From(e)
	}
}
