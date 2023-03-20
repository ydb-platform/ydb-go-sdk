package xsql

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dsn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/query"
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
	var binders []query.Binder
	if info.Params.Has("go_auto_bind") {
		binderNames := strings.Split(info.Params.Get("go_auto_bind"), ",")
		for _, binderName := range binderNames {
			switch binderName {
			case "table_path_prefix":
				if !info.Params.Has("go_auto_bind.table_path_prefix") {
					return nil, nil, xerrors.WithStackTrace(
						fmt.Errorf("table_path_prefix bind required 'go_auto_bind.table_path_prefix' param"),
					)
				}
				binders = append(binders, query.TablePathPrefix(info.Params.Get("go_auto_bind.table_path_prefix")))
			case "declare":
				binders = append(binders, query.Declare())
			case "positional":
				binders = append(binders, query.Positional())
			case "numeric":
				binders = append(binders, query.Numeric())
			case "origin":
				binders = append(binders, query.Origin())
			default:
				return nil, nil, xerrors.WithStackTrace(
					fmt.Errorf("unknown bind type: %s", binderName),
				)
			}
		}
	}
	connectorOpts = append(connectorOpts, WithQueryBinders(binders...))
	return opts, connectorOpts, nil
}
