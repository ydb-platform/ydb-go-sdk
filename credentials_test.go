package ydb

import (
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
)

func TestCredentialsString(t *testing.T) {
	for _, test := range []struct {
		c credentials.Credentials
		s string
	}{
		{
			nil,
			"",
		},
		{
			credentials.NewAnonymousCredentials(""),
			"anonymousCredentials",
		},
		{
			credentials.NewAnonymousCredentials("test"),
			"anonymousCredentials created from test",
		},
		{
			credentials.NewAuthTokenCredentials("", ""),
			"AuthTokenCredentials",
		},
		{
			credentials.NewAuthTokenCredentials("", "test"),
			"AuthTokenCredentials created from test",
		},
	} {
		t.Run(test.s, func(t *testing.T) {
			if stringer, ok := test.c.(fmt.Stringer); ok {
				internal.Equal(t, test.s, stringer.String())
			}
		})

	}
}
