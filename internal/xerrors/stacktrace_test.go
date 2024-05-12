package xerrors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

var (
	errFmtErrorf      = errors.New("fmt.Errorf")
	errFmtErrorPrintf = errors.New("fmt.Errorf Printf")
	errErrorsNew      = errors.New("errors.New")
)

func TestStackTraceError(t *testing.T) {
	for _, test := range []struct {
		error error
		text  string
	}{
		{
			error: WithStackTrace(errFmtErrorf),
			//nolint:lll
			text: "fmt.Errorf at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestStackTraceError(stacktrace_test.go:24)`",
		},
		{
			error: WithStackTrace(errFmtErrorPrintf),
			//nolint:lll
			text: "fmt.Errorf Printf at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestStackTraceError(stacktrace_test.go:29)`",
		},
		{
			error: WithStackTrace(
				WithStackTrace(errErrorsNew),
			),
			//nolint:lll
			text: "errors.New at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestStackTraceError(stacktrace_test.go:35)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestStackTraceError(stacktrace_test.go:34)`",
		},
	} {
		t.Run(test.text, func(t *testing.T) {
			require.Equal(t, test.text, test.error.Error())
		})
	}
}

func TestTransportStackTraceError(t *testing.T) {
	for _, test := range []struct {
		error error
		text  string
	}{
		{
			error: WithStackTrace(
				Transport(
					grpcStatus.Error(grpcCodes.Aborted, "some error"),
					WithAddress("example.com"),
				),
			),
			//nolint:lll
			text: "transport/Aborted (code = 10, source error = \"rpc error: code = Aborted desc = some error\", address: \"example.com\") at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestTransportStackTraceError(stacktrace_test.go:53)`",
		},
		{
			error: WithStackTrace(
				WithStackTrace(
					Transport(
						grpcStatus.Error(grpcCodes.Aborted, "some error"),
					),
				),
			),
			//nolint:lll
			text: "transport/Aborted (code = 10, source error = \"rpc error: code = Aborted desc = some error\") at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestTransportStackTraceError(stacktrace_test.go:64)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestTransportStackTraceError(stacktrace_test.go:63)`",
		},
		{
			error: WithStackTrace(
				WithStackTrace(
					WithStackTrace(
						Transport(
							grpcStatus.Error(grpcCodes.Aborted, "some error"),
						),
					),
				),
			),
			//nolint:lll
			text: "transport/Aborted (code = 10, source error = \"rpc error: code = Aborted desc = some error\") at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestTransportStackTraceError(stacktrace_test.go:76)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestTransportStackTraceError(stacktrace_test.go:75)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestTransportStackTraceError(stacktrace_test.go:74)`",
		},
	} {
		t.Run(test.text, func(t *testing.T) {
			require.Equal(t, test.text, test.error.Error())
		})
	}
}
