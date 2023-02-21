package xsql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
)

func TestParse(t *testing.T) {
	newConnector := func(opts ...ConnectorOption) *Connector {
		c := &Connector{}
		for _, opt := range opts {
			if err := opt(c); err != nil {
				panic(err)
			}
		}
		return c
	}
	for _, tt := range []struct {
		dsn              string
		expOpts          []config.Option
		expConnectorOpts []ConnectorOption
		expErr           error
	}{
		{
			dsn: "grpc://localhost:2135/local",
			expOpts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			expConnectorOpts: nil,
			expErr:           nil,
		},
		{
			dsn: "grpcs://localhost:2135/local/db",
			expOpts: []config.Option{
				config.WithSecure(true),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local/db"),
			},
			expConnectorOpts: nil,
			expErr:           nil,
		},
		{
			dsn: "grpc://localhost:2135/local?query_mode=scripting",
			expOpts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			expConnectorOpts: []ConnectorOption{
				WithDefaultQueryMode(ScriptingQueryMode),
			},
			expErr: nil,
		},
		{
			dsn: "grpc://localhost:2135/local?query_mode=scripting&bind_params=true&table_path_prefix=path/to/tables",
			expOpts: []config.Option{
				config.WithSecure(false),
				config.WithEndpoint("localhost:2135"),
				config.WithDatabase("/local"),
			},
			expConnectorOpts: []ConnectorOption{
				WithDefaultQueryMode(ScriptingQueryMode),
				WithBindings(
					bind.TablePathPrefix("path/to/tables"),
					bind.Params(),
				),
			},
			expErr: nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			opts, connectorOpts, err := Parse(tt.dsn)
			if tt.expErr != nil {
				require.ErrorIs(t, err, tt.expErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, config.New(tt.expOpts...), config.New(opts...))
				require.Equal(t, newConnector(tt.expConnectorOpts...), newConnector(connectorOpts...))
			}
		})
	}
}
