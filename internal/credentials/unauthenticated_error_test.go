package credentials

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

var _ Credentials = customCredentials{}

type customCredentials struct {
	token string
}

func (c customCredentials) Token(ctx context.Context) (string, error) {
	return c.token, nil
}

func TestUnauthenticatedError(t *testing.T) {
	for _, tt := range []struct {
		err         error
		errorString string
	}{
		{
			err: UnauthenticatedError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(NewAnonymousCredentials(WithSourceInfo(""))),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"Anonymous()\"" +
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestUnauthenticatedError(unauthenticated_error_test.go:27)`", //nolint:lll
		},
		{
			err: UnauthenticatedError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(NewAnonymousCredentials(WithSourceInfo(t.Name()))),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"Anonymous(from:\\\"TestUnauthenticatedError\\\")\"" +
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestUnauthenticatedError(unauthenticated_error_test.go:42)`", //nolint:lll
		},
		{
			err: UnauthenticatedError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(NewAccessTokenCredentials("SECRET_TOKEN", WithSourceInfo(""))),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"AccessToken(token:\\\"****(CRC-32c: 9B7801F4)\\\")\"" +
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestUnauthenticatedError(unauthenticated_error_test.go:57)`", //nolint:lll
		},
		{
			err: UnauthenticatedError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(NewAccessTokenCredentials("SECRET_TOKEN", WithSourceInfo(t.Name()))),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"AccessToken(token:\\\"****(CRC-32c: 9B7801F4)\\\",from:\\\"TestUnauthenticatedError\\\")\"" + //nolint:lll
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestUnauthenticatedError(unauthenticated_error_test.go:72)`", //nolint:lll
		},
		{
			err: UnauthenticatedError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(
					NewStaticCredentials("USER", "SECRET_PASSWORD", "auth.endpoint:2135",
						WithSourceInfo(""),
					),
				),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"Static(user:\\\"USER\\\",password:\\\"SEC**********RD\\\",token:\\\"****(CRC-32c: 00000000)\\\")\"" + //nolint:lll
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestUnauthenticatedError(unauthenticated_error_test.go:87)`", //nolint:lll
		},
		{
			err: UnauthenticatedError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(
					NewStaticCredentials("USER", "SECRET_PASSWORD", "auth.endpoint:2135",
						WithSourceInfo(t.Name()),
					),
				),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"Static(user:\\\"USER\\\",password:\\\"SEC**********RD\\\",token:\\\"****(CRC-32c: 00000000)\\\",from:\\\"TestUnauthenticatedError\\\")\"" + //nolint:lll
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestUnauthenticatedError(unauthenticated_error_test.go:106)`", //nolint:lll
		},
		{
			err: UnauthenticatedError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(customCredentials{token: "SECRET_TOKEN"}),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.customCredentials\"" +
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestUnauthenticatedError(unauthenticated_error_test.go:125)`", //nolint:lll
		},
		{
			err: UnauthenticatedError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(NewAnonymousCredentials(WithSourceInfo(""))),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"Anonymous()\"" +
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestUnauthenticatedError(unauthenticated_error_test.go:140)`", //nolint:lll
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.errorString, tt.err.Error())
		})
	}
}
