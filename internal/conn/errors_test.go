package conn

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestIsBadConn(t *testing.T) {
	for i, tt := range []struct {
		err           error
		goodConnCodes []grpcCodes.Code
		badConn       bool
	}{
		{
			err:     fmt.Errorf("test"),
			badConn: false,
		},
		{
			err:     xerrors.Operation(),
			badConn: false,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, "")),
			badConn: false,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.OK, "")),
			badConn: false,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Canceled, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Unknown, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.InvalidArgument, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.DeadlineExceeded, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.NotFound, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.AlreadyExists, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.PermissionDenied, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.FailedPrecondition, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Aborted, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.OutOfRange, "")),
			badConn: false,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Unimplemented, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Internal, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.DataLoss, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Unauthenticated, "")),
			badConn: true,
		},
		{
			err: xerrors.Transport(grpcStatus.Error(grpcCodes.Unauthenticated, "")),
			goodConnCodes: []grpcCodes.Code{
				grpcCodes.Unauthenticated,
			},
			badConn: false,
		},
	} {
		t.Run(fmt.Sprintf("%d. %v", i, tt.err), func(t *testing.T) {
			require.Equal(t, tt.badConn, IsBadConn(tt.err, tt.goodConnCodes...))
			require.Equal(t, tt.badConn, IsBadConn(xerrors.WithStackTrace(tt.err), tt.goodConnCodes...))
			require.Equal(t, tt.badConn, IsBadConn(xerrors.Retryable(tt.err), tt.goodConnCodes...))
		})
	}
}
