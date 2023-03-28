package xerrors

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func TestRetryableCode(t *testing.T) {
	for _, tt := range []struct {
		err  error
		code int32
	}{
		{
			err:  Retryable(fmt.Errorf("some")),
			code: -1,
		},
		{
			err: Retryable(
				Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
			),
			code: int32(grpcCodes.Unavailable),
		},
		{
			err: Retryable(
				Operation(WithStatusCode(Ydb.StatusIds_BAD_REQUEST)),
			),
			code: int32(Ydb.StatusIds_BAD_REQUEST),
		},
		{
			err: Retryable(Retryable(
				Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
			)),
			code: int32(grpcCodes.Unavailable),
		},
		{
			err: Retryable(Retryable(
				Operation(WithStatusCode(Ydb.StatusIds_BAD_REQUEST)),
			)),
			code: int32(Ydb.StatusIds_BAD_REQUEST),
		},
	} {
		t.Run("", func(t *testing.T) {
			var err Error
			require.ErrorAs(t, tt.err, &err)
			require.Equal(t, tt.code, err.Code())
		})
	}
}
