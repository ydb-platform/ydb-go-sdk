package credentials

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCredentialsString(t *testing.T) {
	for _, test := range []struct {
		c Credentials
		s string
	}{
		{
			NewAnonymousCredentials(),
			"Anonymous{From:\"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestCredentialsString(credentials_test.go:16)\"}", //nolint:lll
		},
		{
			NewAnonymousCredentials(WithSourceInfo("test")),
			"Anonymous{From:\"test\"}",
		},
		{
			NewAccessTokenCredentials("123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
			"AccessToken{Token:\"1234****WXYZ(CRC-32c: 81993EA5)\",From:\"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestCredentialsString(credentials_test.go:24)\"}", //nolint:lll
		},
		{
			NewAccessTokenCredentials("123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ", WithSourceInfo("test")),
			"AccessToken{Token:\"1234****WXYZ(CRC-32c: 81993EA5)\",From:\"test\"}",
		},
	} {
		t.Run(test.s, func(t *testing.T) {
			if stringer, ok := test.c.(fmt.Stringer); ok {
				require.Equal(t, test.s, stringer.String())
			}
		})
	}
}
