package xsql

import (
	"context"
	"database/sql/driver"
	"time"
)

var (
	_ driver.ConnBeginTx       = (*Conn)(nil)
	_ driver.NamedValueChecker = (*Conn)(nil)
	_ driver.Pinger            = (*Conn)(nil)

	_ interface {
		Engine() Engine
	} = (*Conn)(nil)

	_ interface {
		LastUsage() time.Time
	} = (*Conn)(nil)

	_ interface {
		GetDatabaseName() string
	} = (*Conn)(nil)

	_ interface {
		Version(ctx context.Context) (version string, err error)
	} = (*Conn)(nil)

	_ interface {
		IsTableExists(ctx context.Context, tableName string) (tableExists bool, err error)
	} = (*Conn)(nil)

	_ interface {
		IsColumnExists(ctx context.Context, tableName string, columnName string) (columnExists bool, err error)
	} = (*Conn)(nil)

	_ interface {
		IsPrimaryKey(ctx context.Context, tableName string, columnName string) (ok bool, err error)
	} = (*Conn)(nil)

	_ interface {
		GetColumns(ctx context.Context, tableName string) (columns []string, err error)
	} = (*Conn)(nil)

	_ interface {
		GetColumnType(ctx context.Context, tableName string, columnName string) (dataType string, err error)
	} = (*Conn)(nil)

	_ interface {
		GetPrimaryKeys(ctx context.Context, tableName string) (pkCols []string, err error)
	} = (*Conn)(nil)

	_ interface {
		GetTables(ctx context.Context, folder string, recursive bool, excludeSysDirs bool) (tables []string, err error)
	} = (*Conn)(nil)

	_ interface {
		GetIndexes(ctx context.Context, tableName string) (indexes []string, err error)
	} = (*Conn)(nil)

	_ interface {
		GetIndexColumns(ctx context.Context, tableName string, indexName string) (columns []string, err error)
	} = (*Conn)(nil)
)
