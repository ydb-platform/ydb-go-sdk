package xsql

import "context"

var (
	_ interface {
		Version(context.Context) (string, error)
	} = (*conn)(nil)

	_ interface {
		IsTableExists(context.Context, string) (bool, error)
	} = (*conn)(nil)

	_ interface {
		IsColumnExists(context.Context, string, string) (bool, error)
	} = (*conn)(nil)

	_ interface {
		IsPrimaryKey(context.Context, string, string) (bool, error)
	} = (*conn)(nil)

	_ interface {
		GetColumns(context.Context, string) ([]string, error)
	} = (*conn)(nil)

	_ interface {
		GetColumnType(context.Context, string, string) (string, error)
	} = (*conn)(nil)

	_ interface {
		GetPrimaryKeys(context.Context, string) ([]string, error)
	} = (*conn)(nil)

	_ interface {
		GetTables(context.Context, string) ([]string, error)
	} = (*conn)(nil)

	_ interface {
		GetIndexes(context.Context, string) ([]string, error)
	} = (*conn)(nil)

	_ interface {
		GetIndexColumns(context.Context, string, string) ([]string, error)
	} = (*conn)(nil)
)
