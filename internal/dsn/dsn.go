package dsn

import (
	"fmt"
	"net/url"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
)

var (
	parsers = map[string]Parser{
		"database": func(database string) ([]config.Option, error) {
			return []config.Option{
				config.WithDatabase(database),
			}, nil
		},
	}
	schemasSecure = map[string]bool{
		"":      true,
		"grpcs": true,
		"grpc":  false,
	}
	errSchemeNotValid = fmt.Errorf("schema not valid")
	errParserExists   = fmt.Errorf("already exists parser. newest parser replaced old. param")
)

type Parser func(value string) ([]config.Option, error)

func Register(param string, parser Parser) error {
	_, has := parsers[param]
	parsers[param] = parser
	if has {
		return errors.WithStackTrace(fmt.Errorf("%w: %v", errParserExists, param))
	}
	return nil
}

func Parse(dsn string) (options []config.Option, err error) {
	uri, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if _, has := schemasSecure[uri.Scheme]; !has {
		return nil, errors.WithStackTrace(fmt.Errorf("%w: %v", errSchemeNotValid, uri.Scheme))
	}
	options = append(
		options,
		config.WithEndpoint(uri.Host),
		config.WithSecure(schemasSecure[uri.Scheme]),
	)
	for param, values := range uri.Query() {
		if p, has := parsers[param]; has {
			for _, v := range values {
				var parsed []config.Option
				if parsed, err = p(v); err != nil {
					return nil, err
				}
				options = append(
					options,
					parsed...,
				)
			}
		}
	}
	return options, err
}
