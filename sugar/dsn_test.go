package sugar

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dsn"
)

func TestDSN(t *testing.T) {
	for _, test := range []struct {
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
		t.Run(test.dsn, func(t *testing.T) {
			s := DSN(test.endpoint, test.database, test.secure)
			if s != test.dsn {
				t.Fatalf("Unexpected result: %s, exp: %s", s, test.dsn)
			}
			opts, err := dsn.Parse(s)
			if err != nil {
				t.Fatalf("")
			}
			config := config.New(opts...)
			if config.Endpoint() != test.endpoint {
				t.Fatalf("Unexpected endpoint: %s, exp: %s", config.Endpoint(), test.endpoint)
			}
			if config.Database() != test.database {
				t.Fatalf("Unexpected database: %s, exp: %s", config.Database(), test.database)
			}
			if config.Secure() != test.secure {
				t.Fatalf("Unexpected secure flag: %v, exp: %v", config.Secure(), test.secure)
			}
		})
	}
}
