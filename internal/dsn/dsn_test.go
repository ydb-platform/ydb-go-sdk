package dsn

import (
	"strconv"
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
	} {
		t.Run(test.connectionString, func(t *testing.T) {
			info, err := Parse(test.connectionString)
			if err != nil {
				t.Fatalf("Received unexpected error:\n%+v", err)
			}
			c := config.New(
				config.WithEndpoint(info.Endpoint),
				config.WithDatabase(info.Database),
				config.WithSecure(info.Secure),
			).With(info.Options...)
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

func TestRegister(t *testing.T) {
	var test1, test2, test3 int
	_ = Register("test1", func(value string) (_ []config.Option, err error) {
		test1, err = strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		return []config.Option{}, nil
	})
	_ = Register("test2", func(value string) (_ []config.Option, err error) {
		test2, err = strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		return []config.Option{}, nil
	})
	_, err := Parse("grpc://ydb-ru.yandex.net:2135/ru/home/gvit/mydb?test1=1&test2=2&test3=3")
	require.NoError(t, err, "")
	require.Equal(t, 1, test1, "")
	require.Equal(t, 2, test2, "")
	require.NotEqualf(t, 3, test3, "")
}
