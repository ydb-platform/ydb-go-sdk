package ydb

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/YandexDatabase/ydb-go-sdk/v2/internal"
)

type Endpoint struct {
	Addr       string
	Port       int
	LoadFactor float32
	Local      bool
}

type discoveryClient struct {
	conn *conn
	meta *meta
}

func (d *discoveryClient) Discover(ctx context.Context, database string, ssl bool) ([]Endpoint, error) {
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
		ctx, d.conn.conn, internal.WrapOpResponse(&resp),
		"/Ydb.Discovery.V1.DiscoveryService/ListEndpoints", &req, &res,
	)
	if err != nil {
		return nil, err
	}
	es := make([]Endpoint, 0, len(res.Endpoints))
	for _, e := range res.Endpoints {
		if e.Ssl == ssl {
			es = append(es, Endpoint{
				Addr:  e.Address,
				Port:  int(e.Port),
				Local: e.Location == res.SelfLocation,
			})
		}
	}
	return es, nil
}
