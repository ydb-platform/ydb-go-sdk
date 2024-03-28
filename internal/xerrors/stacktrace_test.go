package xerrors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func TestStackTraceError(t *testing.T) {
	for _, test := range []struct {
		error error
		text  string
	}{
		{
			error: WithStackTrace(fmt.Errorf("fmt.Errorf")),
			//nolint:lll
			text: "fmt.Errorf at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestStackTraceError(stacktrace_test.go:19)`",
		},
		{
			error: WithStackTrace(fmt.Errorf("fmt.Errorf %s", "Printf")),
			//nolint:lll
			text: "fmt.Errorf Printf at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestStackTraceError(stacktrace_test.go:24)`",
		},
		{
			error: WithStackTrace(
				WithStackTrace(errors.New("errors.New")),
			),
			//nolint:lll
			text: "errors.New at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestStackTraceError(stacktrace_test.go:30)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestStackTraceError(stacktrace_test.go:29)`",
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
			text: "transport/Aborted (code = 10, source error = \"rpc error: code = Aborted desc = some error\", address: \"example.com\") at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestTransportStackTraceError(stacktrace_test.go:48)`",
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
			text: "transport/Aborted (code = 10, source error = \"rpc error: code = Aborted desc = some error\") at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestTransportStackTraceError(stacktrace_test.go:59)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestTransportStackTraceError(stacktrace_test.go:58)`",
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
			text: "transport/Aborted (code = 10, source error = \"rpc error: code = Aborted desc = some error\") at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestTransportStackTraceError(stacktrace_test.go:71)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestTransportStackTraceError(stacktrace_test.go:70)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors.TestTransportStackTraceError(stacktrace_test.go:69)`",
		},
	} {
		t.Run(test.text, func(t *testing.T) {
			require.Equal(t, test.text, test.error.Error())
		})
	}
}
