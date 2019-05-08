package ydb

import (
	"context"

	"google.golang.org/grpc/metadata"

	discovery "github.com/yandex-cloud/ydb-go-sdk/internal/api/grpc/Ydb_Discovery_V1"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb_Discovery"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb_Operations"
)

type Endpoint struct {
	Addr       string
	Port       int
	LoadFactor float32
}

type discoveryClient struct {
	conn *conn
	meta *meta
}

func (d *discoveryClient) Discover(ctx context.Context, database string) ([]Endpoint, error) {
	var (
		resp Ydb_Operations.GetOperationResponse
		res  Ydb_Discovery.ListEndpointsResult
	)
	req := Ydb_Discovery.ListEndpointsRequest{
		Database: database,
	}
	// Get credentials (token actually) for the request.
	md, err := d.meta.md(ctx)
	if err != nil {
		return nil, err
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	err = invoke(
		ctx, d.conn.conn, &resp,
		discovery.ListEndpoints, &req, &res,
	)
	if err != nil {
		return nil, err
	}
	es := make([]Endpoint, len(res.Endpoints))
	for i, e := range res.Endpoints {
		es[i] = Endpoint{
			Addr: e.Address,
			Port: int(e.Port),
		}
	}
	return es, nil
}
