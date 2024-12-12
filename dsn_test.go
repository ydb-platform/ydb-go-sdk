package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/legacy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/propose"
)

func TestParse(t *testing.T) {
	newConnector := func(opts ...xsql.Option) *xsql.Connector {
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
	newLegacyConn := func(opts ...legacy.Option) *legacy.Conn {
		return legacy.New(context.Background(), nil, nil, opts...)
	}
	newQueryConn := func(opts ...propose.Option) *propose.Conn {
		return propose.New(context.Background(), nil, nil, opts...)
	}
	compareConfigs := func(t *testing.T, lhs, rhs *config.Config) {
		require.Equal(t, lhs.Secure(), rhs.Secure())
		require.Equal(t, lhs.Endpoint(), rhs.Endpoint())
		require.Equal(t, lhs.Database(), rhs.Database())
	}
	for _, tt := range []struct {
		dsn           string
		opts          []config.Option
		connectorOpts []xsql.Option
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
			connectorOpts: []xsql.Option{
				xsql.WithDefaultQueryMode(legacy.ScriptingQueryMode),
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
			connectorOpts: []xsql.Option{
				xsql.WithDefaultQueryMode(legacy.ScriptingQueryMode),
				xsql.WithQueryBind(bind.TablePathPrefix("path/to/tables")),
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
			connectorOpts: []xsql.Option{
				xsql.WithDefaultQueryMode(legacy.ScriptingQueryMode),
				xsql.WithQueryBind(bind.TablePathPrefix("path/to/tables")),
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
			connectorOpts: []xsql.Option{
				xsql.WithDefaultQueryMode(legacy.ScriptingQueryMode),
				xsql.WithQueryBind(bind.TablePathPrefix("path/to/tables")),
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
			connectorOpts: []xsql.Option{
				xsql.WithDefaultQueryMode(legacy.ScriptingQueryMode),
				xsql.WithQueryBind(bind.TablePathPrefix("path/to/tables")),
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
			connectorOpts: []xsql.Option{
				xsql.WithDefaultQueryMode(legacy.ScriptingQueryMode),
				xsql.WithQueryBind(bind.TablePathPrefix("path/to/tables")),
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
			connectorOpts: []xsql.Option{
				xsql.WithDefaultQueryMode(legacy.ScriptingQueryMode),
				xsql.WithQueryBind(bind.TablePathPrefix("path/to/tables")),
				xsql.WithQueryBind(bind.PositionalArgs{}),
				xsql.WithQueryBind(bind.AutoDeclare{}),
			},
			err: nil,
		},
		{
			dsn: "grpc://localhost:2135/local?go_fake_tx=scripting,scheme",
			opts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			connectorOpts: []xsql.Option{
				WithFakeTx(ScriptingQueryMode),
				WithFakeTx(SchemeQueryMode),
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
				d, err := driverFromOptions(context.Background(), opts...)
				require.NoError(t, err)
				exp := newConnector(tt.connectorOpts...)
				act := newConnector(d.databaseSQLOptions...)
				t.Run("tableOptions", func(t *testing.T) {
					require.Equal(t, newLegacyConn(exp.LegacyOpts...), newLegacyConn(act.LegacyOpts...))
				})
				t.Run("queryOptions", func(t *testing.T) {
					require.Equal(t, newQueryConn(exp.Options...), newQueryConn(act.Options...))
				})
				exp.LegacyOpts = nil
				exp.Options = nil
				act.LegacyOpts = nil
				act.Options = nil
				require.Equal(t, exp.Bindings(), act.Bindings())
				require.Equal(t, exp, act)
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
