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
	opts = appendTokenOptions(opts, info.Params.Get("token"))

	balancer := info.Params.Get("go_balancer")
	if balancer == "" {
		balancer = info.Params.Get("balancer")
	}
	opts = appendBalancerOptions(opts, balancer)

	queryMode := info.Params.Get("go_query_mode")
	if queryMode == "" {
		queryMode = info.Params.Get("query_mode")
	}
	queryModeOpts, err := appendQueryModeOptions(queryMode)
	if err != nil {
		return nil, nil, err
	}
	connectorOpts = append(connectorOpts, queryModeOpts...)

	fakeTxOpts, err := appendFakeTxOptions(info.Params.Get("go_fake_tx"))
	if err != nil {
		return nil, nil, err
	}
	connectorOpts = append(connectorOpts, fakeTxOpts...)

	binders, err := appendQueryBindOptions(info.Params.Get("go_query_bind"))
	if err != nil {
		return nil, nil, err
	}
	connectorOpts = append(connectorOpts, binders...)

	return opts, connectorOpts, nil
}

func appendTokenOptions(opts []config.Option, token string) []config.Option {
	if token != "" {
		opts = append(opts, config.WithCredentials(credentials.NewAccessTokenCredentials(token)))
	}
	return opts
}

func appendBalancerOptions(opts []config.Option, balancer string) []config.Option {
	if balancer != "" {
		opts = append(opts, config.WithBalancer(balancers.FromConfig(balancer)))
	}
	return opts
}

func appendQueryModeOptions(queryMode string) ([]ConnectorOption, error) {
	var connectorOpts []ConnectorOption
	if queryMode != "" {
		mode := QueryModeFromString(queryMode)
		if mode == UnknownQueryMode {
			return nil, xerrors.WithStackTrace(fmt.Errorf("unknown query mode '%s'", queryMode))
		}
		connectorOpts = append(connectorOpts, WithDefaultQueryMode(mode))
	}
	return connectorOpts, nil
}

func appendFakeTxOptions(fakeTx string) ([]ConnectorOption, error) {
	var connectorOpts []ConnectorOption
	for _, queryModeStr := range strings.Split(fakeTx, ",") {
		if queryModeStr == "" {
			continue
		}
		mode := QueryModeFromString(queryModeStr)
		if mode == UnknownQueryMode {
			return nil, xerrors.WithStackTrace(fmt.Errorf("unknown query mode '%s' in fakeTx option", queryModeStr))
		}
		connectorOpts = append(connectorOpts, WithFakeTx(mode))
	}
	return connectorOpts, nil
}

func appendQueryBindOptions(queryBind string) ([]ConnectorOption, error) {
	var binders []ConnectorOption
	if queryBind != "" {
		queryTransformers := strings.Split(queryBind, ",")
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
						return nil, xerrors.WithStackTrace(fmt.Errorf("error extracting table path prefix from '%s': %v", transformer, err))
					}
					binders = append(binders, WithTablePathPrefix(prefix))
				} else {
					return nil, xerrors.WithStackTrace(fmt.Errorf("unknown query transformer '%s'", transformer))
				}
			}
		}
	}
	return binders, nil
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
