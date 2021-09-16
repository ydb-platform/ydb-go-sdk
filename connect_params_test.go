package ydb

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
)

func TestParseConnectionString(t *testing.T) {
	for _, test := range []struct {
		connectionString string
		schema           string
		endpoint         string
		database         string
		error            error
	}{
		{
			"grpc://ydb-ru.yandex.net:2135/?database=/ru/home/gvit/mydb",
			"grpc",
			"ydb-ru.yandex.net:2135",
			"/ru/home/gvit/mydb",
			nil,
		},
		{
			"grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
			"grpcs",
			"ydb.serverless.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
			nil,
		},
		{
			"grpcs://lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135/?database=/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			"grpcs",
			"lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			nil,
		},
		{
			"abcd://ydb-ru.yandex.net:2135/?database=/ru/home/gvit/mydb",
			"",
			"",
			"",
			errSchemeNotValid,
		},
	} {
		t.Run(test.connectionString, func(t *testing.T) {
			schema, endpoint, database, err := parseConnectionString(test.connectionString)
			if !errors.Is(err, test.error) {
				t.Fatal(fmt.Sprintf("Received unexpected error:\n%+v", err))
			}
			internal.Equal(t, test.schema, schema)
			internal.Equal(t, test.endpoint, endpoint)
			internal.Equal(t, test.database, database)
		})
	}
}

func assertConnectParams(t *testing.T, params ConnectParams) {
	internal.NotNil(t, params)
	internal.Equal(t, "endpoint", params.Endpoint())
	internal.Equal(t, "database", params.Database())
	if !params.UseTLS() {
		t.Fatal("UseTLS is not true")
	}
}

func TestEndpointDatabase(t *testing.T) {
	params := EndpointDatabase("endpoint", "database", true)
	assertConnectParams(t, params)
}

func TestMustConnectionString(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			t.Fatal("panic on MustConnectionString", e)
		}
	}()
	params := MustConnectionString("grpcs://endpoint/?database=database")
	assertConnectParams(t, params)
}
