package xsql

import (
	"fmt"
	"net/url"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func Parse(dataSourceName string) (opts []config.Option, connectorOpts []ConnectorOption, err error) {
	uri, err := url.Parse(dataSourceName)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}
	if token := uri.Query().Get("token"); token != "" {
		opts = append(opts, config.WithCredentials(credentials.NewAccessTokenCredentials(token)))
	}
	if balancer := uri.Query().Get("balancer"); balancer != "" {
		opts = append(opts, config.WithBalancer(balancers.FromConfig(balancer)))
	}
	if queryMode := uri.Query().Get("query_mode"); queryMode != "" {
		mode := QueryModeFromString(queryMode)
		if mode == UnknownQueryMode {
			return nil, nil, xerrors.WithStackTrace(fmt.Errorf("unknown query mode: %s", queryMode))
		}
		connectorOpts = append(connectorOpts, WithDefaultQueryMode(mode))
	}
	if tablePathPrefix := uri.Query().Get("table_path_prefix"); tablePathPrefix != "" {
		connectorOpts = append(connectorOpts, WithTablePathPrefix(tablePathPrefix))
	}
	return opts, connectorOpts, nil
}
