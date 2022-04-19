package sugar

// Usage of this package
//
// db, err := ydb.Open(
//   ctx,
//   sugar.DSN("endpoint", "database", false),
// )

type connectionString struct {
	endpoint string
	database string
	secure   bool
}

func (cs connectionString) Build() (s string) {
	s = "grpc"
	if cs.secure {
		s += "s"
	}
	return s + "://" + cs.endpoint + "/?database=" + cs.database
}

// DSN makes connection string (data source name) by endpoint, database and secure
func DSN(endpoint, database string, secure bool) (s string) {
	cs := connectionString{
		endpoint: endpoint,
		database: database,
		secure:   secure,
	}
	return cs.Build()
}
