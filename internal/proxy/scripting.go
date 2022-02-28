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

func (d *proxyScripting) Execute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (_ result.Result, err error) {
	ctx, err = d.meta.Meta(ctx)
	if err != nil {
		return nil, err
	}
	return d.client.Execute(ctx, query, params)
}

func (d *proxyScripting) Explain(
	ctx context.Context,
	query string,
	mode scripting.ExplainMode,
) (e table.ScriptingYQLExplanation, err error) {
	ctx, err = d.meta.Meta(ctx)
	if err != nil {
		return e, err
	}
	return d.client.Explain(ctx, query, mode)
}

func (d *proxyScripting) StreamExecute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (_ result.StreamResult, err error) {
	ctx, err = d.meta.Meta(ctx)
	if err != nil {
		return nil, err
	}
	return d.client.StreamExecute(ctx, query, params)
}

func Scripting(client scripting.Client, meta meta.Meta) scripting.Client {
	return &proxyScripting{
		client: client,
		meta:   meta,
	}
}

func (d *proxyScripting) Close(ctx context.Context) (err error) {
	// nop
	return nil
}
