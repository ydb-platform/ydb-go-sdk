package dsn

import (
	"fmt"
)

// Usage of this package
//
// db, err := ydb.New(
//   ctx,
//   ydb.WithConnectionString(
//     New("endpoint", "database").WithSecure(false).String(),
//   ),
// )

// DSN helps to make connection string from separated endpoint and database
type DSN interface {
	fmt.Stringer

	// WithSecure makes new DSN from current DSN with passed secure flag
	WithSecure(secure bool) DSN
}

type connectionString struct {
	endpoint string
	database string
	secure   bool
}

func (cs *connectionString) String() (s string) {
	s = "grpc"
	if cs.secure {
		s += "s"
	}
	return s + "://" + cs.endpoint + "/?database=" + cs.database
}

func (cs *connectionString) WithSecure(secure bool) DSN {
	return &connectionString{
		secure:   secure,
		endpoint: cs.endpoint,
		database: cs.database,
	}
}

// New makes secured DSN with endpoint and database
func New(endpoint, database string) DSN {
	return &connectionString{
		endpoint: endpoint,
		database: database,
		secure:   true,
	}
}
