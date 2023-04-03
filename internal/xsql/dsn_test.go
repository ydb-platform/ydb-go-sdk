package xsql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
)

func TestParse(t *testing.T) {
	newConnector := func(opts ...ConnectorOption) *Connector {
		c := &Connector{}
		for _, opt := range opts {
			if err := opt.Apply(c); err != nil {
				t.Error(err)
			}
		}
		return c
	}
	compareConfigs := func(t *testing.T, lhs, rhs config.Config) {
		require.Equal(t, lhs.Secure(), rhs.Secure())
		require.Equal(t, lhs.Endpoint(), rhs.Endpoint())
		require.Equal(t, lhs.Database(), rhs.Database())
	}
	for _, tt := range []struct {
		dsn           string
		opts          []config.Option
		connectorOpts []ConnectorOption
		err           error
	}{
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
			connectorOpts: []ConnectorOption{
				WithDefaultQueryMode(ScriptingQueryMode),
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
			connectorOpts: []ConnectorOption{
				WithDefaultQueryMode(ScriptingQueryMode),
				WithQueryBindAndPathNormalizer(query.TablePathPrefixBind("path/to/tables")),
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
			connectorOpts: []ConnectorOption{
				WithDefaultQueryMode(ScriptingQueryMode),
				WithQueryBindAndPathNormalizer(query.TablePathPrefixBind("path/to/tables")),
				WithQueryBind(query.NumericArgsBind{}),
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
			connectorOpts: []ConnectorOption{
				WithDefaultQueryMode(ScriptingQueryMode),
				WithQueryBindAndPathNormalizer(query.TablePathPrefixBind("path/to/tables")),
				WithQueryBind(query.PositionalArgsBind{}),
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
			connectorOpts: []ConnectorOption{
				WithDefaultQueryMode(ScriptingQueryMode),
				WithQueryBindAndPathNormalizer(query.TablePathPrefixBind("path/to/tables")),
				WithQueryBind(query.AutoDeclareBind{}),
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
			connectorOpts: []ConnectorOption{
				WithDefaultQueryMode(ScriptingQueryMode),
				WithQueryBindAndPathNormalizer(query.TablePathPrefixBind("path/to/tables")),
			},
			err: nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			opts, connectorOpts, err := Parse(tt.dsn)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, newConnector(tt.connectorOpts...), newConnector(connectorOpts...))
				compareConfigs(t, config.New(tt.opts...), config.New(opts...))
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
