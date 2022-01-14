package credentials

import (
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestCredentialsString(t *testing.T) {
	for _, test := range []struct {
		c Credentials
		s string
	}{
		{
			nil,
			"without credentials",
		},
		{
			NewAnonymousCredentials(""),
			"anonymous",
		},
		{
			NewAnonymousCredentials("test"),
			"anonymous created from test",
		},
		{
			NewAccessTokenCredentials("", ""),
			"accessToken",
		},
		{
			NewAccessTokenCredentials("", "test"),
			"accessToken created from test",
		},
	} {
		t.Run(test.s, func(t *testing.T) {
			if stringer, ok := test.c.(fmt.Stringer); ok {
				testutil.Equal(t, test.s, stringer.String())
			}
		})
	}
}
