package xerrors

import (
	"errors"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

var (
	errEmpty           = errors.New("")
	errTestError       = errors.New("TestError")
	errTestErrorPrintf = errors.New("TestErrorPrintf")
)

func TestIsYdb(t *testing.T) {
	for _, test := range []struct {
		error      error
		isYdbError bool
	}{
		{
			error:      nil,
			isYdbError: false,
		},
		{
			error:      Operation(WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
			isYdbError: true,
		},
		{
			error:      Transport(grpcStatus.Error(grpcCodes.DeadlineExceeded, "")),
			isYdbError: true,
		},
		{
			error:      RetryableError(errEmpty),
			isYdbError: false,
		},
		{
			error:      WithStackTrace(Operation(WithStatusCode(Ydb.StatusIds_BAD_SESSION))),
			isYdbError: true,
		},
		{
			error:      WithStackTrace(Transport(grpcStatus.Error(grpcCodes.DeadlineExceeded, ""))),
			isYdbError: true,
		},
		{
			error:      WithStackTrace(RetryableError(errEmpty)),
			isYdbError: false,
		},
		{
			error:      WithStackTrace(WithStackTrace(Operation(WithStatusCode(Ydb.StatusIds_BAD_SESSION)))),
			isYdbError: true,
		},
		{
			error:      WithStackTrace(WithStackTrace(Transport(grpcStatus.Error(grpcCodes.DeadlineExceeded, "")))),
			isYdbError: true,
		},
		{
			error:      WithStackTrace(WithStackTrace(RetryableError(errEmpty))),
			isYdbError: false,
		},
		{
			error:      errTestErrorPrintf,
			isYdbError: false,
		},
		{
			error:      errTestError,
			isYdbError: false,
		},
		{
			error:      Wrap(errTestErrorPrintf),
			isYdbError: true,
		},
		{
			error:      Wrap(errTestError),
			isYdbError: true,
		},
	} {
		t.Run("", func(t *testing.T) {
			if IsYdb(test.error) != test.isYdbError {
				t.Fatalf("unexpected check ydb error: %v, want: %v", IsYdb(test.error), test.isYdbError)
			}
		})
	}
}
