package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
)

func TestCredentialsString(t *testing.T) {
	for _, test := range []struct {
		c credentials.Credentials
		s string
	}{
		{
			nil,
			"without credentials",
		},
		{
			credentials.NewAnonymousCredentials(""),
			"anonymous",
		},
		{
			credentials.NewAnonymousCredentials("test"),
			"anonymous created from test",
		},
		{
			credentials.NewAccessTokenCredentials("", ""),
			"accessToken",
		},
		{
			credentials.NewAccessTokenCredentials("", "test"),
			"accessToken created from test",
		},
	} {
		t.Run(test.s, func(t *testing.T) {
			if stringer, ok := test.c.(fmt.Stringer); ok {
				require.Equal(t, test.s, stringer.String())
			}
		})
	}
}
