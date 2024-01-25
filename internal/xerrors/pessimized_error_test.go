package xerrors

import (
	"context"
	"errors"
	"fmt"
	"testing"

	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func TestMustPessimizeEndpoint(t *testing.T) {
	for _, test := range []struct {
		error     error
		pessimize bool
	}{
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Canceled, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Unknown, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.InvalidArgument, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.DeadlineExceeded, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.NotFound, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.AlreadyExists, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.PermissionDenied, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, "")),
			pessimize: false,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.FailedPrecondition, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Aborted, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.OutOfRange, "")),
			pessimize: false,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Unimplemented, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Internal, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.DataLoss, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Unauthenticated, "")),
			pessimize: true,
		},
		{
			error:     context.Canceled,
			pessimize: false,
		},
		{
			error:     context.DeadlineExceeded,
			pessimize: false,
		},
		{
			error:     fmt.Errorf("user error"),
			pessimize: false,
		},
	} {
		err := errors.Unwrap(test.error)
		if err == nil {
			err = test.error
		}
		t.Run(err.Error(), func(t *testing.T) {
			pessimize := MustPessimizeEndpoint(test.error)
			if pessimize != test.pessimize {
				t.Errorf("unexpected pessimization status for error `%v`: %t, exp: %t", test.error, pessimize, test.pessimize)
			}
		})
	}
}
