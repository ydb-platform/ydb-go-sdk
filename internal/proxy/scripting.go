package proxy

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

type proxyScripting struct {
	client scripting.Client
	meta   meta.Meta
}

func (d *proxyScripting) ExecuteYql(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (_ result.Result, err error) {
	ctx, err = d.meta.Meta(ctx)
	if err != nil {
		return nil, err
	}
	return d.client.ExecuteYql(ctx, query, params)
}

func (d *proxyScripting) ExplainYql(
	ctx context.Context,
	query string,
	mode scripting.ExplainMode,
) (e table.ScriptingYQLExplanation, err error) {
	ctx, err = d.meta.Meta(ctx)
	if err != nil {
		return e, err
	}
	return d.client.ExplainYql(ctx, query, mode)
}

func (d *proxyScripting) StreamExecuteYql(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (_ result.StreamResult, err error) {
	ctx, err = d.meta.Meta(ctx)
	if err != nil {
		return nil, err
	}
	return d.client.StreamExecuteYql(ctx, query, params)
}

func Scripting(client scripting.Client, meta meta.Meta) scripting.Client {
	return &proxyScripting{
		client: client,
		meta:   meta,
	}
}

func (d *proxyScripting) Close(ctx context.Context) (err error) {
	ctx, err = d.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return d.client.Close(ctx)
}
