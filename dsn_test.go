package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
)

func TestParse(t *testing.T) {
	newConnector := func(opts ...xsql.ConnectorOption) *xsql.Connector {
		c := &xsql.Connector{}
		for _, opt := range opts {
			if opt != nil {
				if err := opt.Apply(c); err != nil {
					t.Error(err)
				}
			}
		}

		return c
	}
	compareConfigs := func(t *testing.T, lhs, rhs *config.Config) {
		require.Equal(t, lhs.Secure(), rhs.Secure())
		require.Equal(t, lhs.Endpoint(), rhs.Endpoint())
		require.Equal(t, lhs.Database(), rhs.Database())
	}
	for _, tt := range []struct {
		dsn           string
		opts          []config.Option
		connectorOpts []xsql.ConnectorOption
		err           error
	}{
		{
			dsn: "grpc://localhost:2135/local?go_fake_tx=scripting,scheme",
			opts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			connectorOpts: []xsql.ConnectorOption{
				xsql.WithFakeTx(xsql.ScriptingQueryMode),
				xsql.WithFakeTx(xsql.SchemeQueryMode),
			},
			err: nil,
		},
		{
			dsn: "grpc://localhost:2135/local",
			opts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			connectorOpts: nil,
			err:           nil,
		},
		{
			dsn: "grpcs://localhost:2135/local/db",
			opts: []config.Option{
				config.WithSecure(true),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local/db"),
			},
			connectorOpts: nil,
			err:           nil,
		},
		{
			dsn: "grpc://localhost:2135/local?query_mode=scripting",
			opts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			connectorOpts: []xsql.ConnectorOption{
				xsql.WithDefaultQueryMode(xsql.ScriptingQueryMode),
			},
			err: nil,
		},
		{
			dsn: "grpc://localhost:2135/local?query_mode=scripting&go_query_bind=table_path_prefix(path/to/tables)",
			opts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			connectorOpts: []xsql.ConnectorOption{
				xsql.WithDefaultQueryMode(xsql.ScriptingQueryMode),
				xsql.WithTablePathPrefix("path/to/tables"),
			},
			err: nil,
		},
		{
			dsn: "grpc://localhost:2135/local?query_mode=scripting&go_query_bind=table_path_prefix(path/to/tables),numeric", //nolint:lll
			opts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			connectorOpts: []xsql.ConnectorOption{
				xsql.WithDefaultQueryMode(xsql.ScriptingQueryMode),
				xsql.WithTablePathPrefix("path/to/tables"),
				xsql.WithQueryBind(bind.NumericArgs{}),
			},
			err: nil,
		},
		{
			dsn: "grpc://localhost:2135/local?query_mode=scripting&go_query_bind=table_path_prefix(path/to/tables),positional", //nolint:lll
			opts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			connectorOpts: []xsql.ConnectorOption{
				xsql.WithDefaultQueryMode(xsql.ScriptingQueryMode),
				xsql.WithTablePathPrefix("path/to/tables"),
				xsql.WithQueryBind(bind.PositionalArgs{}),
			},
			err: nil,
		},
		{
			dsn: "grpc://localhost:2135/local?query_mode=scripting&go_query_bind=table_path_prefix(path/to/tables),declare",
			opts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			connectorOpts: []xsql.ConnectorOption{
				xsql.WithDefaultQueryMode(xsql.ScriptingQueryMode),
				xsql.WithTablePathPrefix("path/to/tables"),
				xsql.WithQueryBind(bind.AutoDeclare{}),
			},
			err: nil,
		},
		{
			dsn: "grpc://localhost:2135/local?query_mode=scripting&go_query_bind=table_path_prefix(path/to/tables)",
			opts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			connectorOpts: []xsql.ConnectorOption{
				xsql.WithDefaultQueryMode(xsql.ScriptingQueryMode),
				xsql.WithTablePathPrefix("path/to/tables"),
			},
			err: nil,
		},
		{
			dsn: "grpc://localhost:2135/local?query_mode=scripting&go_query_bind=positional,declare,table_path_prefix(path/to/tables)", //nolint:lll
			opts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			connectorOpts: []xsql.ConnectorOption{
				xsql.WithDefaultQueryMode(xsql.ScriptingQueryMode),
				xsql.WithTablePathPrefix("path/to/tables"),
				xsql.WithQueryBind(bind.PositionalArgs{}),
				xsql.WithQueryBind(bind.AutoDeclare{}),
			},
			err: nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			opts, err := parseConnectionString(tt.dsn)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
				d, err := newConnectionFromOptions(context.Background(), opts...)
				require.NoError(t, err)
				require.Equal(t, newConnector(tt.connectorOpts...), newConnector(d.databaseSQLOptions...))
				compareConfigs(t, config.New(tt.opts...), d.config)
			}
		})
	}
}

func TestExtractTablePathPrefixFromBinderName(t *testing.T) {
	for _, tt := range []struct {
		binderName      string
		tablePathPrefix string
		err             error
	}{
		{
			binderName:      "table_path_prefix(path/to/tables)",
			tablePathPrefix: "path/to/tables",
		},
		{
			binderName:      "table_path_prefix()",
			tablePathPrefix: "",
			err:             errWrongTablePathPrefix,
		},
		{
			binderName:      "table_path_prefix",
			tablePathPrefix: "",
			err:             errWrongTablePathPrefix,
		},
		{
			binderName:      "TablePathPrefix(path/to/tables)",
			tablePathPrefix: "",
			err:             errWrongTablePathPrefix,
		},
	} {
		t.Run("", func(t *testing.T) {
			tablePathPrefix, err := extractTablePathPrefixFromBinderName(tt.binderName)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.tablePathPrefix, tablePathPrefix)
			}
		})
	}
}
