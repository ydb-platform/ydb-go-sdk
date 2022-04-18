package dsn

// Usage of this package
//
// db, err := ydb.Open(
//   ctx,
//   dsn.Make("endpoint", "database", dsn.WithSecure(false)),
// )

type connectionString struct {
	endpoint string
	database string
	secure   bool
}

type option func(cs *connectionString)

// WithSecure changes default secure flag
func WithSecure(secure bool) option {
	return func(cs *connectionString) {
		cs.secure = secure
	}
}

// Make makes connection string by endpoint, database and options
//
// Options provides additional parameters of DSN
func Make(endpoint, database string, opts ...option) (s string) {
	cs := connectionString{
		endpoint: endpoint,
		database: database,
		secure:   true,
	}
	for _, o := range opts {
		o(&cs)
	}
	s = "grpc"
	if cs.secure {
		s += "s"
	}
	return s + "://" + cs.endpoint + "/?database=" + cs.database
}
