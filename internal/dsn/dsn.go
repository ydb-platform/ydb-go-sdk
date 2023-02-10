package dsn

import (
	"fmt"
	"net/url"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	parsers = map[string]Parser{
		// for compatibility with old connection string format
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

type UserInfo struct {
	User     string
	Password string
}

type parsedInfo struct {
	Endpoint string
	Database string
	Secure   bool
	UserInfo *UserInfo
	Options  []config.Option
}

func Parse(dsn string) (info parsedInfo, err error) {
	uri, err := url.Parse(dsn)
	if err != nil {
		return info, xerrors.WithStackTrace(err)
	}
	if port := uri.Port(); port == "" {
		return info, xerrors.WithStackTrace(fmt.Errorf("bad connection string '%s': port required", dsn))
	}
	info.Endpoint = uri.Host
	info.Database = uri.Path
	info.Secure = uri.Scheme != insecureSchema
	if uri.User != nil {
		password, _ := uri.User.Password()
		info.UserInfo = &UserInfo{
			User:     uri.User.Username(),
			Password: password,
		}
	}
	for param, values := range uri.Query() {
		if p, has := parsers[param]; has {
			for _, v := range values {
				var parsed []config.Option
				if parsed, err = p(v); err != nil {
					return info, xerrors.WithStackTrace(err)
				}
				info.Options = append(info.Options, parsed...)
			}
		}
	}
	return info, nil
}
