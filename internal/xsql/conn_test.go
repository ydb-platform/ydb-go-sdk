package xsql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
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
		GetTables(ctx context.Context, absPath string) (tables []string, err error)
	} = (*conn)(nil)

	_ interface {
		GetAllTables(ctx context.Context, absPath string) (tables []string, err error)
	} = (*conn)(nil)

	_ interface {
		GetIndexes(ctx context.Context, tableName string) (indexes []string, err error)
	} = (*conn)(nil)

	_ interface {
		GetIndexColumns(ctx context.Context, tableName string, indexName string) (columns []string, err error)
	} = (*conn)(nil)
)

func Test_hasDeclare(t *testing.T) {
	for _, tt := range []struct {
		query      string
		hasDeclare bool
	}{
		{
			query:      "",
			hasDeclare: false,
		},
		{
			query:      "SELECT 1",
			hasDeclare: false,
		},
		{
			query:      "DECLARE",
			hasDeclare: false,
		},
		{
			query:      "DECLARE v AS Utf8",
			hasDeclare: false,
		},
		{
			query:      "DECLARE v AS Utf8;",
			hasDeclare: false,
		},
		{
			query:      "DECLARE $v AS Utf8",
			hasDeclare: false,
		},
		{
			query:      "DECLARE $v AS Utf8;",
			hasDeclare: true,
		},
		{
			query:      "declare $v as Utf8;",
			hasDeclare: true,
		},
		{
			query:      "declare $v as Utf8 ;",
			hasDeclare: true,
		},
		{
			query: `
				DECLARE $v AS
				Utf8;`,
			hasDeclare: true,
		},
		{
			query: `
				DECLARE 
					$v
				AS
					Utf8;`,
			hasDeclare: true,
		},
		{
			query: `
				DECLARE $v
					AS				Utf8;`,
			hasDeclare: true,
		},
		{
			query: `
				DECLARE 
					$v AS Utf8;`,
			hasDeclare: true,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.hasDeclare, hasDeclare(tt.query), fmt.Sprintf("%q must be %v, but not", tt.query, tt.hasDeclare))
		})
	}
}

func Test_hasTablePathPrefix(t *testing.T) {
	for _, tt := range []struct {
		query              string
		hasTablePathPrefix bool
	}{
		{
			query:              "",
			hasTablePathPrefix: false,
		},
		{
			query:              "SELECT 1",
			hasTablePathPrefix: false,
		},
		{
			query:              "PRAGMA",
			hasTablePathPrefix: false,
		},
		{
			query:              "PRAGMA TablePathPrefix(\"/test/db/path/prefix\");",
			hasTablePathPrefix: true,
		},
		{
			query:              "PRAGMA TablePathPrefix(\"/test/db/path/prefix\") ;",
			hasTablePathPrefix: true,
		},
		{
			query:              "PRAGMA  TablePathPrefix(\"/test/db/path/prefix\");",
			hasTablePathPrefix: true,
		},
		{
			query:              "PRAGMA TablePathPrefix( \"/test/db/path/prefix\");",
			hasTablePathPrefix: true,
		},
		{
			query:              "PRAGMA TablePathPrefix(\"/test/db/path/prefix\" );",
			hasTablePathPrefix: true,
		},
		{
			query:              "PRAGMA TablePathPrefix(\"/test/db/path/prefix\") ;",
			hasTablePathPrefix: true,
		},
		{
			query:              "PRAGMA  TablePathPrefix( \"/test/db/path/prefix\" ) ;",
			hasTablePathPrefix: true,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.hasTablePathPrefix, hasTablePathPrefix(tt.query),
				fmt.Sprintf("%q must be %v, but not", tt.query, tt.hasTablePathPrefix),
			)
		})
	}
}
