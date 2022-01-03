package ydb

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestParseConnectionString(t *testing.T) {
	for _, test := range []struct {
		connectionString string
		schema           string
		endpoint         string
		database         string
		token            string
		error            error
	}{
		{
			"grpc://ydb-ru.yandex.net:2135/?" +
				"database=/ru/home/gvit/mydb&token=123",
			"grpc",
			"ydb-ru.yandex.net:2135",
			"/ru/home/gvit/mydb",
			"123",
			nil,
		},
		{
			"grpcs://ydb.serverless.yandexcloud.net:2135/?" +
				"database=/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1&token=123",
			"grpcs",
			"ydb.serverless.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
			"123",
			nil,
		},
		{
			"grpcs://lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135/?" +
				"database=/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv&token=123",
			"grpcs",
			"lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			"123",
			nil,
		},
		{
			"abcd://ydb-ru.yandex.net:2135/?" +
				"database=/ru/home/gvit/mydb",
			"",
			"",
			"",
			"",
			errSchemeNotValid,
		},
	} {
		t.Run(test.connectionString, func(t *testing.T) {
			schema, endpoint, database, token, err := parseConnectionString(test.connectionString)
			if !errors.Is(err, test.error) {
				t.Fatal(fmt.Sprintf("Received unexpected error:\n%+v", err))
			}
			testutil.Equal(t, test.schema, schema)
			testutil.Equal(t, test.endpoint, endpoint)
			testutil.Equal(t, test.database, database)
			testutil.Equal(t, test.token, token)
		})
	}
}
