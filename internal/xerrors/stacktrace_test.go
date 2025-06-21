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

func TestPrintWithoutStack(t *testing.T) {
	for _, tt := range []struct {
		in  error
		out string
	}{
		{
			in:  errors.New("test"),
			out: "test",
		},
		{
			in:  WithStackTrace(errors.New("test")),
			out: "test",
		},
		{
			in:  WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, "test")),
			out: "rpc error: code = Aborted desc = test",
		},
		{
			in: fmt.Errorf("test1: %w",
				WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, "test2")),
			),
			out: "test1: rpc error: code = Aborted desc = test2",
		},
		{
			in: fmt.Errorf("test1: %w",
				WithStackTrace(
					errors.New("test2"),
				),
			),
			out: "test1: test2",
		},
		{
			in: fmt.Errorf("test1: %w",
				WithStackTrace(
					fmt.Errorf("test2: %w",
						WithStackTrace(
							errors.New("test3"),
						),
					),
				),
			),
			out: "test1: test2: test3",
		},
	} {
		t.Run(tt.in.Error(), func(t *testing.T) {
			require.Equal(t, tt.out, PrintWithoutStack(tt.in))
		})
	}
}

func TestRemoveStack(t *testing.T) {
	require.Equal(t,
		"pool.With failed with 33 attempts: [\"attempt No.33: context deadline exceeded\",\"query: operation/UNAVAILABLE (code = 400050, address = localhost:2135, issues = [{'Failed to resolve tablet: 72075186234645162 after several retries'}])\",\"query: operation/UNAVAILABLE (code = 400050, address = localhost:2135, issues = [{'Failed to resolve tablet: 72075186234644725 after several retries'}])\"]", //nolint:lll
		removeStack("pool.With failed with 33 attempts: [\"attempt No.33: context deadline exceeded\",\"query: operation/UNAVAILABLE (code = 400050, address = localhost:2135, issues = [{'Failed to resolve tablet: 72075186234645162 after several retries'}]) at `github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*grpcClientStream).RecvMsg(grpc_client_stream.go:180)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.nextPart(result.go:221)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*streamResult).nextPart(result.go:203)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.newResult(result.go:166)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.execute(execute_query.go:138)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).execute(session.go:149)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Query(session.go:197)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.do.func1(client.go:214)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).try(pool.go:465)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).With.func2(pool.go:493)` at `github.com/ydb-platform/ydb-go-sdk/v3/retry.Retry.func1(retry.go:264)` at `github.com/ydb-platform/ydb-go-sdk/v3/retry.opWithRecover(retry.go:418)`\",\"query: operation/UNAVAILABLE (code = 400050, address = localhost:2135, issues = [{'Failed to resolve tablet: 72075186234644725 after several retries'}]) at `github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*grpcClientStream).RecvMsg(grpc_client_stream.go:180)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.nextPart(result.go:221)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*streamResult).nextPart(result.go:203)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.newResult(result.go:166)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.execute(execute_query.go:138)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).execute(session.go:149)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Query(session.go:197)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.do.func1(client.go:214)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).try(pool.go:465)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).With.func2(pool.go:493)` at `github.com/ydb-platform/ydb-go-sdk/v3/retry.Retry.func1(retry.go:264)` at `github.com/ydb-platform/ydb-go-sdk/v3/retry.opWithRecover(retry.go:418)`\"] at `github.com/ydb-platform/ydb-go-sdk/v3/retry.RetryWithResult(retry.go:374)` at `github.com/ydb-platform/ydb-go-sdk/v3/retry.Retry(retry.go:270)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).With(pool.go:499)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.do(client.go:222)`"), //nolint:lll
	)
}
