package common

import (
	"context"
	"database/sql/driver"
)

// Rows is the internal abstraction for query results between xquery / xtable and
// the database/sql facade in internal/xsql. Iteration respects the caller's
// context (QueryContext deadline / cancel).
//
// The driver-facing shapes (driver.Rows without context arguments on Next,
// RowsNextResultSet, optional column typing interfaces, etc.) are implemented
// only in internal/xsql, which pins QueryContext onto every Rows call site.
type Rows interface {
	Columns(ctx context.Context) []string

	ColumnTypeDatabaseTypeName(ctx context.Context, index int) string
	ColumnTypeNullable(ctx context.Context, index int) (nullable, ok bool)

	Next(ctx context.Context, dst []driver.Value) error
	NextResultSet(ctx context.Context) error

	HasNextResultSet(ctx context.Context) bool

	Close() error
}
