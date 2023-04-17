package xsql

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dsn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

const tablePathPrefixTransformer = "table_path_prefix"

func Parse(dataSourceName string) (opts []config.Option, connectorOpts []ConnectorOption, _ error) {
	info, err := dsn.Parse(dataSourceName)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}
	opts = append(opts, info.Options...)
	if token := info.Params.Get("token"); token != "" {
		opts = append(opts, config.WithCredentials(credentials.NewAccessTokenCredentials(token)))
	}
	if balancer := info.Params.Get("go_balancer"); balancer != "" {
		opts = append(opts, config.WithBalancer(balancers.FromConfig(balancer)))
	} else if balancer := info.Params.Get("balancer"); balancer != "" {
		opts = append(opts, config.WithBalancer(balancers.FromConfig(balancer)))
	}
	if queryMode := info.Params.Get("go_query_mode"); queryMode != "" {
		mode := QueryModeFromString(queryMode)
		if mode == UnknownQueryMode {
			return nil, nil, xerrors.WithStackTrace(fmt.Errorf("unknown query mode: %s", queryMode))
		}
		connectorOpts = append(connectorOpts, WithDefaultQueryMode(mode))
	} else if queryMode := info.Params.Get("query_mode"); queryMode != "" {
		mode := QueryModeFromString(queryMode)
		if mode == UnknownQueryMode {
			return nil, nil, xerrors.WithStackTrace(fmt.Errorf("unknown query mode: %s", queryMode))
		}
		connectorOpts = append(connectorOpts, WithDefaultQueryMode(mode))
	}
	if fakeTx := info.Params.Get("go_fake_tx"); fakeTx != "" {
		for _, queryMode := range strings.Split(fakeTx, ",") {
			mode := QueryModeFromString(queryMode)
			if mode == UnknownQueryMode {
				return nil, nil, xerrors.WithStackTrace(fmt.Errorf("unknown query mode: %s", queryMode))
			}
			connectorOpts = append(connectorOpts, WithFakeTx(mode))
		}
	}
	if info.Params.Has("go_query_bind") {
		var binders []ConnectorOption
		queryTransformers := strings.Split(info.Params.Get("go_query_bind"), ",")
		for _, transformer := range queryTransformers {
			switch transformer {
			case "declare":
				binders = append(binders, WithQueryBind(bind.AutoDeclare{}))
			case "positional":
				binders = append(binders, WithQueryBind(bind.PositionalArgs{}))
			case "numeric":
				binders = append(binders, WithQueryBind(bind.NumericArgs{}))
			default:
				if strings.HasPrefix(transformer, tablePathPrefixTransformer) {
					prefix, err := extractTablePathPrefixFromBinderName(transformer)
					if err != nil {
						return nil, nil, xerrors.WithStackTrace(err)
					}
					binders = append(binders, WithTablePathPrefix(prefix))
				} else {
					return nil, nil, xerrors.WithStackTrace(
						fmt.Errorf("unknown query rewriter: %s", transformer),
					)
				}
			}
		}
		connectorOpts = append(connectorOpts, binders...)
	}
	return opts, connectorOpts, nil
}

var (
	tablePathPrefixRe       = regexp.MustCompile(tablePathPrefixTransformer + "\\((.*)\\)")
	errWrongTablePathPrefix = errors.New("wrong '" + tablePathPrefixTransformer + "' query transformer")
)

func extractTablePathPrefixFromBinderName(binderName string) (string, error) {
	ss := tablePathPrefixRe.FindAllStringSubmatch(binderName, -1)
	if len(ss) != 1 || len(ss[0]) != 2 || ss[0][1] == "" {
		return "", xerrors.WithStackTrace(fmt.Errorf("%w: %s", errWrongTablePathPrefix, binderName))
	}
	return ss[0][1], nil
}
