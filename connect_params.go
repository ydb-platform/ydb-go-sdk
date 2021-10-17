package ydb

import (
	"errors"
	"fmt"
	"net/url"
)

var (
	_insecureProtocol = "grpc"
	_secureProtocol   = "grpcs"

	validSchemas = map[string]struct{}{
		_insecureProtocol: {},
		_secureProtocol:   {},
	}

	errSchemeNotValid = errors.New("scheme not valid")
)

func isSecure(schema string) bool {
	return schema == _secureProtocol
}

func parseConnectionString(connection string) (schema string, endpoint string, database string, token string, _ error) {
	uri, err := url.Parse(connection)
	if err != nil {
		return "", "", "", "", err
	}
	if _, has := validSchemas[uri.Scheme]; !has {
		return "", "", "", "", fmt.Errorf("%s: %w", uri.Scheme, errSchemeNotValid)
	}
	return uri.Scheme, uri.Host, uri.Query().Get("database"), uri.Query().Get("token"), err
}

type ConnectParams interface {
	Endpoint() string
	Database() string
	Secure() bool
	Token() string
}

type connectParams struct {
	endpoint string
	database string
	secure   bool
	token    string
}

func (c connectParams) Endpoint() string {
	return c.endpoint
}

func (c connectParams) Database() string {
	return c.database
}

func (c connectParams) Secure() bool {
	return c.secure
}

func (c connectParams) Token() string {
	return c.token
}

func EndpointDatabase(endpoint string, database string, secure bool) ConnectParams {
	return &connectParams{
		endpoint: endpoint,
		database: database,
		secure:   secure,
	}
}

func ConnectionString(uri string) (ConnectParams, error) {
	if uri == "" {
		return nil, fmt.Errorf("empty URI")
	}
	schema, endpoint, database, token, err := parseConnectionString(uri)
	if err != nil {
		return nil, err
	}
	return &connectParams{
		endpoint: endpoint,
		database: database,
		secure:   isSecure(schema),
		token:    token,
	}, nil
}

func MustConnectionString(uri string) ConnectParams {
	connectionParams, err := ConnectionString(uri)
	if err != nil {
		panic(err)
	}
	return connectionParams
}
