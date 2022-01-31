package proxy

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

type proxyScripting struct {
	client ydb_scripting.Client
	meta   meta.Meta
}

func (d *proxyScripting) Execute(
	ctx context.Context,
	query string,
	params *ydb_table.QueryParameters,
) (_ ydb_table_result.Result, err error) {
	ctx, err = d.meta.Meta(ctx)
	if err != nil {
		return nil, err
	}
	return d.client.Execute(ctx, query, params)
}

func (d *proxyScripting) Explain(
	ctx context.Context,
	query string,
	mode ydb_scripting.ExplainMode,
) (e ydb_table.ScriptingYQLExplanation, err error) {
	ctx, err = d.meta.Meta(ctx)
	if err != nil {
		return e, err
	}
	return d.client.Explain(ctx, query, mode)
}

func (d *proxyScripting) StreamExecute(
	ctx context.Context,
	query string,
	params *ydb_table.QueryParameters,
) (_ ydb_table_result.StreamResult, err error) {
	ctx, err = d.meta.Meta(ctx)
	if err != nil {
		return nil, err
	}
	return d.client.StreamExecute(ctx, query, params)
}

func Scripting(client ydb_scripting.Client, meta meta.Meta) ydb_scripting.Client {
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
