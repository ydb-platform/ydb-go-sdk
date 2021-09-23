package credentials

import (
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/assert"
	"testing"
)

func TestCredentialsString(t *testing.T) {
	for _, test := range []struct {
		c Credentials
		s string
	}{
		{
			nil,
			"",
		},
		{
			NewAnonymousCredentials(""),
			"anonymousCredentials",
		},
		{
			NewAnonymousCredentials("test"),
			"anonymousCredentials created from test",
		},
		{
			NewAuthTokenCredentials("", ""),
			"AuthTokenCredentials",
		},
		{
			NewAuthTokenCredentials("", "test"),
			"AuthTokenCredentials created from test",
		},
	} {
		t.Run(test.s, func(t *testing.T) {
			if stringer, ok := test.c.(fmt.Stringer); ok {
				assert.Equal(t, test.s, stringer.String())
			}
		})

	}
}
