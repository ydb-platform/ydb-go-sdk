package common

import "database/sql/driver"

type Rows interface {
	driver.RowsNextResultSet
	driver.RowsColumnTypeDatabaseTypeName
	driver.RowsColumnTypeNullable
}
