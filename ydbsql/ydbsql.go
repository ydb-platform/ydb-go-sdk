package ydbsql

import (
	"database/sql"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func init() {
	sql.Register("ydb", new(legacyDriver))
}

type legacyDriver struct {
}

func (d *legacyDriver) OpenConnector(connection string) (driver.Connector, error) {
	return Connector(With(ydb.WithConnectionString(connection)))
}

func (d *legacyDriver) Open(name string) (driver.Conn, error) {
	return nil, ErrDeprecated
}
