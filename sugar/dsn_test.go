package sugar

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dsn"
)

func TestDSN(t *testing.T) {
	for _, tt := range []struct {
		endpoint string
		database string
		secure   bool
		dsn      string
	}{
		{
			"localhost:2135",
			"/local",
			false,
			"grpc://localhost:2135/local",
		},
		{
			"ydb-ru.yandex.net:2135",
			"/ru/home/gvit/mydb",
			false,
			"grpc://ydb-ru.yandex.net:2135/ru/home/gvit/mydb",
		},
		{
			"ydb.serverless.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
			true,
			"grpcs://ydb.serverless.yandexcloud.net:2135/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
		},
		{
			"lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			true,
			"grpcs://lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
		},
	} {
		t.Run(tt.dsn, func(t *testing.T) {
			s := DSN(tt.endpoint, tt.database, tt.secure)
			if s != tt.dsn {
				t.Fatalf("Unexpected result: %s, exp: %s", s, tt.dsn)
			}
			info, err := dsn.Parse(s)
			if err != nil {
				t.Fatalf("")
			}
			lhs, rhs := config.New(info.Options...), config.New(
				config.WithSecure(tt.secure),
				config.WithEndpoint(tt.endpoint),
				config.WithDatabase(tt.database),
			)
			require.Equal(t, lhs.Endpoint(), rhs.Endpoint())
			require.Equal(t, lhs.Database(), rhs.Database())
			require.Equal(t, lhs.Secure(), rhs.Secure())
		})
	}
}
