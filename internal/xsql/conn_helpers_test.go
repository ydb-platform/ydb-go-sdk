package xsql

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
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

func TestConn_Engine(t *testing.T) {
	tests := []struct {
		name     string
		engine   Engine
		expected Engine
	}{
		{
			name:     "QUERY",
			engine:   QUERY,
			expected: QUERY,
		},
		{
			name:     "TABLE",
			engine:   TABLE,
			expected: TABLE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &Conn{
				processor: tt.engine,
			}
			require.Equal(t, tt.expected, conn.Engine())
		})
	}
}

func TestConn_LastUsage(t *testing.T) {
	conn := &Conn{
		lastUsage: xsync.NewLastUsage(),
	}

	// Initially should be zero time
	lastUsage := conn.LastUsage()
	require.False(t, lastUsage.IsZero())

	// Start and stop usage
	done := conn.lastUsage.Start()
	time.Sleep(10 * time.Millisecond)
	done()

	// Should have updated time
	newLastUsage := conn.LastUsage()
	require.True(t, newLastUsage.After(lastUsage) || newLastUsage.Equal(lastUsage))
}

func TestConn_normalizePath(t *testing.T) {
	tests := []struct {
		name       string
		pathPrefix string
		tableName  string
		expected   string
	}{
		{
			name:       "WithPrefix",
			pathPrefix: "/local",
			tableName:  "test_table",
			expected:   "/local/test_table",
		},
		{
			name:       "AlreadyPrefixed",
			pathPrefix: "/local",
			tableName:  "/local/test_table",
			expected:   "/local/test_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &Conn{
				connector: &Connector{
					pathNormalizer: bind.TablePathPrefix(tt.pathPrefix),
				},
			}

			result := conn.normalizePath(tt.tableName)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConn_Version(t *testing.T) {
	conn := &Conn{}
	version, err := conn.Version(context.Background())
	require.NoError(t, err)
	require.Equal(t, "default", version)
}

func TestIsSysDir(t *testing.T) {
	tests := []struct {
		name         string
		databaseName string
		dirAbsPath   string
		expected     bool
	}{
		{
			name:         "SysDirExact",
			databaseName: "/local",
			dirAbsPath:   "/local/.sys",
			expected:     true,
		},
		{
			name:         "SysDirSubpath",
			databaseName: "/local",
			dirAbsPath:   "/local/.sys/something",
			expected:     true,
		},
		{
			name:         "SysHealthDirExact",
			databaseName: "/local",
			dirAbsPath:   "/local/.sys_health",
			expected:     true,
		},
		{
			name:         "SysHealthDirSubpath",
			databaseName: "/local",
			dirAbsPath:   "/local/.sys_health/metrics",
			expected:     true,
		},
		{
			name:         "NormalDir",
			databaseName: "/local",
			dirAbsPath:   "/local/tables",
			expected:     false,
		},
		{
			name:         "DifferentDatabase",
			databaseName: "/local",
			dirAbsPath:   "/other/.sys",
			expected:     false,
		},
		{
			name:         "EmptyPath",
			databaseName: "/local",
			dirAbsPath:   "",
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSysDir(tt.databaseName, tt.dirAbsPath)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConn_toYdb(t *testing.T) {
	conn := &Conn{
		connector: &Connector{
			bindings: newMockBindings(),
		},
	}

	yql, p, err := conn.toYdb("SELECT 1", driver.NamedValue{Name: "p1", Value: 42})
	require.NoError(t, err)
	require.NotEmpty(t, yql)
	require.NotNil(t, p)
}
