package dsn

import (
	"fmt"
	"net/url"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	parsers = map[string]Parser{
		"database": func(database string) ([]config.Option, error) {
			return []config.Option{
				config.WithDatabase(database),
			}, nil
		},
	}
	insecureSchema  = "grpc"
	errParserExists = xerrors.Wrap(fmt.Errorf("already exists parser. newest parser replaced old. param"))
)

type Parser func(value string) ([]config.Option, error)

func Register(param string, parser Parser) error {
	_, has := parsers[param]
	parsers[param] = parser
	if has {
		return xerrors.WithStackTrace(fmt.Errorf("%w: %v", errParserExists, param))
	}
	return nil
}

func Parse(dsn string) (options []config.Option, err error) {
	uri, err := url.Parse(dsn)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	options = append(
		options,
		config.WithEndpoint(uri.Host),
		config.WithSecure(uri.Scheme != insecureSchema),
	)
	for param, values := range uri.Query() {
		if p, has := parsers[param]; has {
			for _, v := range values {
				var parsed []config.Option
				if parsed, err = p(v); err != nil {
					return nil, xerrors.WithStackTrace(err)
				}
				options = append(
					options,
					parsed...,
				)
			}
		}
	}
	return options, nil
}
