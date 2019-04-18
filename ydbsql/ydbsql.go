package ydbsql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"

	"github.com/yandex-cloud/ydb-go-sdk"
)

func init() {
	sql.Register("ydb", new(legacyDriver))
}

type legacyDriver struct {
}

func (d *legacyDriver) OpenConnector(name string) (driver.Connector, error) {
	u, err := url.ParseRequestURI(name)
	if err != nil {
		return nil, err
	}
	if err := validateURL(u); err != nil {
		return nil, err
	}
	return Connector(urlConnectorOptions(u)...), nil
}

func (d *legacyDriver) Open(name string) (driver.Conn, error) {
	return nil, ErrDeprecated
}

const (
	urlAuthToken = "auth-token"
)

func urlConnectorOptions(u *url.URL) []ConnectorOption {
	return []ConnectorOption{
		WithEndpoint(u.Host),
		WithDatabase(u.Path),
		WithCredentials(ydb.AuthTokenCredentials{
			AuthToken: u.Query().Get(urlAuthToken),
		}),
	}
}

func validateURL(u *url.URL) error {
	if s := u.Scheme; s != "ydb" {
		return fmt.Errorf("malformed source uri: unexpected scheme: %q", s)
	}
	if u.Host == "" {
		return fmt.Errorf("malformed source uri: empty host")
	}
	if u.Path == "" {
		return fmt.Errorf("malformed source uri: empty database path")
	}

	var withToken bool
	for key := range u.Query() {
		if key != urlAuthToken {
			return fmt.Errorf("malformed source uri: unexpected option: %q", key)
		}
		withToken = true
	}
	if !withToken {
		return fmt.Errorf("malformed source uri: empty token")
	}

	return nil
}
