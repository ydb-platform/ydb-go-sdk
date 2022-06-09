package dsn

import (
	"context"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func init() {
	_ = Register("token", func(token string) ([]config.Option, error) {
		return []config.Option{
			config.WithCredentials(
				credentials.NewAccessTokenCredentials(token, ""),
			),
		}, nil
	})
}

func TestParseConnectionString(t *testing.T) {
	for _, test := range []struct {
		connectionString string
		secure           bool
		endpoint         string
		database         string
		token            string
	}{
		{
			"grpc://ydb-ru.yandex.net:2135/?" +
				"database=/ru/home/gvit/mydb&token=123",
			false,
			"ydb-ru.yandex.net:2135",
			"/ru/home/gvit/mydb",
			"123",
		},
		{
			"grpc://ydb-ru.yandex.net:2135/ru/home/gvit/mydb?token=123",
			false,
			"ydb-ru.yandex.net:2135",
			"/ru/home/gvit/mydb",
			"123",
		},
		{
			"grpcs://ydb.serverless.yandexcloud.net:2135/?" +
				"database=/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1&token=123",
			true,
			"ydb.serverless.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
			"123",
		},
		{
			"grpcs://ydb.serverless.yandexcloud.net:2135" +
				"/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1?token=123",
			true,
			"ydb.serverless.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1",
			"123",
		},
		{
			"grpcs://ydb.serverless.yandexcloud.net:2135" +
				"/ru-central1/b1g8skpblkos03malf3s/etn02qso4v3isjb00te1?database=/ru/home/gvit/mydb&token=123",
			true,
			"ydb.serverless.yandexcloud.net:2135",
			"/ru/home/gvit/mydb",
			"123",
		},
		{
			"grpcs://lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135/?" +
				"database=/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv&token=123",
			true,
			"lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			"123",
		},
		{
			"grpcs://lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135" +
				"/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv?token=123",
			true,
			"lb.etn03r9df42nb631unbv.ydb.mdb.yandexcloud.net:2135",
			"/ru-central1/b1g8skpblkos03malf3s/etn03r9df42nb631unbv",
			"123",
		},
		{
			"abcd://ydb-ru.yandex.net:2135/?database=/ru/home/gvit/mydb",
			true,
			"ydb-ru.yandex.net:2135",
			"/ru/home/gvit/mydb",
			"",
		},
		{
			"abcd://ydb-ru.yandex.net:2135/ru/home/gvit/mydb",
			true,
			"ydb-ru.yandex.net:2135",
			"/ru/home/gvit/mydb",
			"",
		},
	} {
		t.Run(test.connectionString, func(t *testing.T) {
			options, err := Parse(test.connectionString)
			if err != nil {
				t.Fatalf("Received unexpected error:\n%+v", err)
			}
			config := config.New(options...)
			testutil.Equal(t, test.secure, config.Secure())
			testutil.Equal(t, test.endpoint, config.Endpoint())
			testutil.Equal(t, test.database, config.Database())
			var token string
			if credentials := config.Credentials(); credentials != nil {
				token, err = credentials.Token(context.Background())
				if err != nil {
					t.Fatalf("Received unexpected error:\n%+v", err)
				}
			} else {
				token = ""
			}
			testutil.Equal(t, test.token, token)
		})
	}
}
