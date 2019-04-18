package ydb

import (
	"context"

	discovery "github.com/yandex-cloud/ydb-go-sdk/internal/api/grpc/Ydb_Discovery_V1"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb_Discovery"
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
	var res Ydb_Discovery.ListEndpointsResult
	req := Ydb_Discovery.ListEndpointsRequest{
		Database: database,
	}
	err := (grpcCaller{
		meta: d.meta,
	}).call(ctx, d.conn, discovery.ListEndpoints, &req, &res)
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
