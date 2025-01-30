package credentials

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var _ Credentials = customCredentials{}

type customCredentials struct {
	token string
}

func (c customCredentials) Token(context.Context) (string, error) {
	return c.token, nil
}

func TestAccessError(t *testing.T) {
	for _, tt := range []struct {
		err         error
		errorString string
	}{
		{
			err: AccessError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(NewAnonymousCredentials(WithSourceInfo(""))),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"Anonymous{}\"" +
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestAccessError(errors_test.go:33)`",
		},
		{
			err: AccessError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(NewAnonymousCredentials(WithSourceInfo(t.Name()))),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"Anonymous{From:\\\"TestAccessError\\\"}\"" +
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestAccessError(errors_test.go:48)`",
		},
		{
			err: AccessError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(NewAccessTokenCredentials("SECRET_TOKEN", WithSourceInfo(""))),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"AccessToken{Token:\\\"****(CRC-32c: 9B7801F4)\\\"}\"" +
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestAccessError(errors_test.go:63)`",
		},
		{
			err: AccessError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(NewAccessTokenCredentials("SECRET_TOKEN", WithSourceInfo(t.Name()))),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"AccessToken{Token:\\\"****(CRC-32c: 9B7801F4)\\\",From:\\\"TestAccessError\\\"}\"" +
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestAccessError(errors_test.go:78)`",
		},
		{
			err: AccessError(
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
				"credentials:\"Static{User:\\\"USER\\\",Password:\\\"SEC**********RD\\\",Token:\\\"****(CRC-32c: 00000000)\\\"}\"" + //nolint:lll
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestAccessError(errors_test.go:93)`",
		},
		{
			err: AccessError(
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
				"credentials:\"Static{User:\\\"USER\\\",Password:\\\"SEC**********RD\\\",Token:\\\"****(CRC-32c: 00000000)\\\",From:\\\"TestAccessError\\\"}\"" + //nolint:lll
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestAccessError(errors_test.go:112)`",
		},
		{
			err: AccessError(
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
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestAccessError(errors_test.go:131)`",
		},
		{
			err: AccessError(
				"something went wrong",
				errors.New("test"),
				WithEndpoint("grps://localhost:2135"),
				WithDatabase("/local"),
				WithCredentials(NewAnonymousCredentials(WithSourceInfo(""))),
			),
			errorString: "something went wrong (" +
				"endpoint:\"grps://localhost:2135\"," +
				"database:\"/local\"," +
				"credentials:\"Anonymous{}\"" +
				"): test " +
				"at `github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials.TestAccessError(errors_test.go:146)`",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.errorString, tt.err.Error())
		})
	}
}

func TestWrongStringifyCustomCredentials(t *testing.T) {
	require.Equal(t, "&{\"SECRET_TOKEN\"}", fmt.Sprintf("%q", &customCredentials{token: "SECRET_TOKEN"}))
}

func TestIsAccessError(t *testing.T) {
	for _, tt := range []struct {
		error error
		is    bool
	}{
		{
			error: grpcStatus.Error(grpcCodes.PermissionDenied, ""),
			is:    true,
		},
		{
			error: grpcStatus.Error(grpcCodes.Unauthenticated, ""),
			is:    true,
		},
		{
			error: xerrors.Transport(grpcStatus.Error(grpcCodes.PermissionDenied, "")),
			is:    true,
		},
		{
			error: xerrors.Transport(grpcStatus.Error(grpcCodes.Unauthenticated, "")),
			is:    true,
		},
		{
			error: xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED)),
			is:    true,
		},
		{
			error: errors.New("some error"),
			is:    false,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.is, IsAccessError(tt.error), tt.error)
		})
	}
}
