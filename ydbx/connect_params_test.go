package ydbx

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParseConnectionString(t *testing.T) {
	for _, test := range []struct {
		connectionString string
		schema           string
		endpoint         string
		databse          string
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
				require.Fail(t, "")
			}
			require.Equal(t, test.schema, schema)
			require.Equal(t, test.endpoint, endpoint)
			require.Equal(t, test.databse, database)
		})
	}
}

func TestEndpointDatabase(t *testing.T) {
	params := EndpointDatabase("endpoint", "database", true)
	require.NotNil(t, params)
	require.Equal(t, "endpoint", params.Endpoint())
	require.Equal(t, "database", params.Database())
	require.True(t, true, params.UseTLS())
}

func TestMustConnectionString(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			require.Fail(t, "panic on MustConnectionString", e)
		}
	}()
	params := MustConnectionString("grpcs://endpoint/?database=database")
	require.NotNil(t, params)
	require.Equal(t, "endpoint", params.Endpoint())
	require.Equal(t, "database", params.Database())
	require.True(t, true, params.UseTLS())
}
