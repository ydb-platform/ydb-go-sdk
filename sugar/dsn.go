package sugar

import "net/url"

type dsnOption func(dsn *url.URL)

// Usage of this package
//
// db, err := ydb.Open(ctx,
//   sugar.DSN("endpoint", "database", false),
// )

// DSN makes connection string (data source name) by endpoint, database and secure
func DSN(endpoint, database string, opts ...dsnOption) (s string) {
	qp := url.Values{}

	dsn := url.URL{
		Scheme:   "grpc",
		Host:     endpoint,
		Path:     database,
		RawQuery: qp.Encode(),
	}

	for _, opt := range opts {
		opt(&dsn)
	}

	return dsn.String()
}

func WithSecure(secure bool) dsnOption {
	return func(dsn *url.URL) {
		if secure {
			dsn.Scheme = "grpcs"
		} else {
			dsn.Scheme = "grpc"
		}
	}
}

func WithUserPassword(user string, password string) dsnOption {
	return func(dsn *url.URL) {
		dsn.User = url.UserPassword(user, password)
	}
}
