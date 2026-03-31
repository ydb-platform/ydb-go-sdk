package dsn

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
)

func TestParseConnectionString(t *testing.T) {
	for _, test := range []struct {
		connectionString string
		secure           bool
		endpoint         string
		database         string
		user             string
		password         string
	}{
		{
			"grpc://ydb-ru.yandex.net:2135/?" +
				"database=/ru/home/gvit/mydb",
			false,
			"ydb-ru.yandex.net:2135",
			"/ru/home/gvit/mydb",
			"",
			"",
		},
		{
			"grpc://ydb-ru.yandex.net:2135/ru/home/gvit/mydb",
			false,
			"ydb-ru.yandex.net:2135",
			"/ru/home/gvit/mydb",
			"",
			"",
		},
		{
			"grpcs://ydb.serverless.yandexcloud.net:2135/?" +
				"database=/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
			true,
			"ydb.serverless.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
			"",
			"",
		},
		{
			"grpcs://ydb.serverless.yandexcloud.net:2135" +
				"/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
			true,
			"ydb.serverless.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
			"",
			"",
		},
		{
			"grpcs://ydb.serverless.yandexcloud.net:2135" +
				"/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1?database=/ru/home/gvit/mydb",
			true,
			"ydb.serverless.yandexcloud.net:2135",
			"/ru/home/gvit/mydb",
			"",
			"",
		},
		{
			"grpcs://lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135/?" +
				"database=/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			true,
			"lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			"",
			"",
		},
		{
			"grpcs://lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135" +
				"/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			true,
			"lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			"",
			"",
		},
		{
			"grpcs://user:password@lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135" +
				"/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			true,
			"lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			"user",
			"password",
		},
		{
			"abcd://ydb-ru.yandex.net:2135/?database=/ru/home/gvit/mydb",
			true,
			"ydb-ru.yandex.net:2135",
			"/ru/home/gvit/mydb",
			"",
			"",
		},
		{
			"abcd://ydb-ru.yandex.net:2135/ru/home/gvit/mydb",
			true,
			"ydb-ru.yandex.net:2135",
			"/ru/home/gvit/mydb",
			"",
			"",
		},
		{
			"localhost:3049/Root",
			true,
			"localhost:3049",
			"/Root",
			"",
			"",
		},
		{
			"grpc://localhost:3049/Root",
			false,
			"localhost:3049",
			"/Root",
			"",
			"",
		},
	} {
		t.Run(test.connectionString, func(t *testing.T) {
			info, err := Parse(test.connectionString)
			if err != nil {
				t.Fatalf("Received unexpected error:\n%+v", err)
			}
			c := config.New(info.Options...)
			require.Equal(t, test.secure, c.Secure())
			require.Equal(t, test.endpoint, c.Endpoint())
			require.Equal(t, test.database, c.Database())
			if test.user != "" {
				require.NotNil(t, t, info.UserInfo)
				require.Equal(t, test.user, info.UserInfo.User)
				require.Equal(t, test.password, info.UserInfo.Password)
			} else {
				require.Nil(t, info.UserInfo)
			}
		})
	}
}

func TestParseConnectionStringEmptyDatabase(t *testing.T) {
	info, err := Parse("grpc://ydb-ru.yandex.net:2135")
	if err != nil {
		t.Fatalf("Received unexpected error:\n%+v", err)
	}
	c := config.New(config.WithDatabase("mydb"))
	c = c.With(info.Options...)
	require.False(t, c.Secure())
	require.Equal(t, "ydb-ru.yandex.net:2135", c.Endpoint())
	require.Equal(t, "mydb", c.Database())
}
