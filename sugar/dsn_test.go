package sugar

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dsn"
)

func TestDSN(t *testing.T) {
	for _, tt := range []struct {
		act string
		exp string
	}{
		{
			DSN("localhost:2135", "/local"),
			"grpc://localhost:2135/local",
		},
		{
			DSN("localhost:2135", "/local", WithUserPassword("user", "")),
			"grpc://user@localhost:2135/local",
		},
		{
			DSN("localhost:2135", "/local", WithUserPassword("user", "password")),
			"grpc://user:password@localhost:2135/local",
		},
		{
			DSN("ydb-ru.yandex.net:2135", "/ru/home/gvit/mydb"),
			"grpc://ydb-ru.yandex.net:2135/ru/home/gvit/mydb",
		},
		{
			DSN("ydb.serverless.yandexcloud.net:2135", "/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1", WithSecure(true)), //nolint:lll
			"grpcs://ydb.serverless.yandexcloud.net:2135/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
		},
		{
			DSN("lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135", "/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv", WithSecure(true)), //nolint:lll
			"grpcs://lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
		},
	} {
		t.Run(tt.exp, func(t *testing.T) {
			act, err := dsn.Parse(tt.act)
			require.NoError(t, err)
			exp, err := dsn.Parse(tt.act)
			require.NoError(t, err)
			require.Equal(t, exp.Params, act.Params)
			require.Equal(t, exp.UserInfo, act.UserInfo)
			lhs, rhs := config.New(act.Options...), config.New(exp.Options...)
			require.Equal(t, lhs.Endpoint(), rhs.Endpoint())
			require.Equal(t, lhs.Database(), rhs.Database())
			require.Equal(t, lhs.Secure(), rhs.Secure())
			require.Equal(t, lhs.Credentials(), rhs.Credentials())
		})
	}
}
