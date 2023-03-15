package xsql

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dsn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/bind"
)

func Parse(dataSourceName string) (opts []config.Option, connectorOpts []ConnectorOption, err error) {
	info, err := dsn.Parse(dataSourceName)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}
	opts = append(opts, info.Options...)
	if token := info.Params.Get("token"); token != "" {
		opts = append(opts, config.WithCredentials(credentials.NewAccessTokenCredentials(token)))
	}
	if balancer := info.Params.Get("balancer"); balancer != "" {
		opts = append(opts, config.WithBalancer(balancers.FromConfig(balancer)))
	}
	if queryMode := info.Params.Get("query_mode"); queryMode != "" {
		mode := QueryModeFromString(queryMode)
		if mode == UnknownQueryMode {
			return nil, nil, xerrors.WithStackTrace(fmt.Errorf("unknown query mode: %s", queryMode))
		}
		connectorOpts = append(connectorOpts, WithDefaultQueryMode(mode))
	}
	b := bind.NoBind()
	if info.Params.Has("go_auto_bind") {
		t, err := bind.FromString(info.Params.Get("go_auto_bind"))
		if err != nil {
			return nil, nil, xerrors.WithStackTrace(err)
		}
		b = bind.SwitchType(b, t)
	}
	if info.Params.Has("go_auto_bind.table_path_prefix") {
		b = b.WithTablePathPrefix(info.Params.Get("go_auto_bind.table_path_prefix"))
	}
	connectorOpts = append(connectorOpts, WithBind(b))
	return opts, connectorOpts, nil
}
