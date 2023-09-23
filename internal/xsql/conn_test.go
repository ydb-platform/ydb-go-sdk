package xsql

import (
	"context"
)

var (
	_ interface {
		GetDatabaseName() string
	} = (*conn)(nil)

	_ interface {
		Version(ctx context.Context) (version string, err error)
	} = (*conn)(nil)

	_ interface {
		IsTableExists(ctx context.Context, tableName string) (tableExists bool, err error)
	} = (*conn)(nil)

	_ interface {
		IsColumnExists(ctx context.Context, tableName string, columnName string) (columnExists bool, err error)
	} = (*conn)(nil)

	_ interface {
		IsPrimaryKey(ctx context.Context, tableName string, columnName string) (ok bool, err error)
	} = (*conn)(nil)

	_ interface {
		GetColumns(ctx context.Context, tableName string) (columns []string, err error)
	} = (*conn)(nil)

	_ interface {
		GetColumnType(ctx context.Context, tableName string, columnName string) (dataType string, err error)
	} = (*conn)(nil)

	_ interface {
		GetPrimaryKeys(ctx context.Context, tableName string) (pkCols []string, err error)
	} = (*conn)(nil)

	_ interface {
		GetTables(ctx context.Context, folder string, recursive bool, excludeSysDirs bool) (tables []string, err error)
	} = (*conn)(nil)

	_ interface {
		GetIndexes(ctx context.Context, tableName string) (indexes []string, err error)
	} = (*conn)(nil)

	_ interface {
		GetIndexColumns(ctx context.Context, tableName string, indexName string) (columns []string, err error)
	} = (*conn)(nil)
)
