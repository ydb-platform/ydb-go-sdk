package sugar

import (
	"fmt"
	"testing"
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
			"grpc://localhost:2135?database=%2Flocal",
		},
		{
			"ydb-ru.yandex.net:2135",
			"/ru/home/gvit/mydb",
			false,
			"grpc://ydb-ru.yandex.net:2135?database=%2Fru%2Fhome%2Fgvit%2Fmydb",
		},
		{
			"ydb.serverless.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
			true,
			"grpcs://ydb.serverless.yandexcloud.net:2135?database=%2Fru-central1%2Fb1g8skpblkos03malf3s%2Fetn02qso4v3isjb00te1",
		},
		{
			"lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			true,
			"grpcs://lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135?database=%2Fru-central1%2Fb1g8skpblkos03malf3s%2Fetn03r9df42nb631unbv",
		},
	} {
		t.Run(test.dsn, func(t *testing.T) {
			dsn := DSN(test.endpoint, test.database, test.secure)
			if dsn != test.dsn {
				t.Fatal(fmt.Sprintf("Unexpected result: %s, exp: %s", dsn, test.dsn))
			}
		})
	}
}
