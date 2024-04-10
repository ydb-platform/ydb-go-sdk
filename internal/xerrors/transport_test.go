package xerrors

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestIsTransportError(t *testing.T) {
	for _, tt := range []struct {
		name  string
		err   error
		codes []grpcCodes.Code
		match bool
	}{
		// check only transport error with any grpc status code
		{
			name:  xtest.CurrentFileLine(),
			err:   grpcStatus.Error(grpcCodes.Canceled, ""),
			match: true,
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   &transportError{status: grpcStatus.New(grpcCodes.Canceled, "")},
			match: true,
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   fmt.Errorf("wrapped: %w", &transportError{status: grpcStatus.New(grpcCodes.Canceled, "")}),
			match: true,
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   fmt.Errorf("wrapped: %w", grpcStatus.Error(grpcCodes.Canceled, "")),
			match: true,
		},
		{
			name: xtest.CurrentFileLine(),
			err: Join(
				fmt.Errorf("test"),
				grpcStatus.Error(grpcCodes.Canceled, ""),
				Retryable(fmt.Errorf("test")),
			),
			match: true,
		},
		// match grpc status code
		{
			name:  xtest.CurrentFileLine(),
			err:   grpcStatus.Error(grpcCodes.Canceled, ""),
			codes: []grpcCodes.Code{grpcCodes.Canceled},
			match: true,
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   &transportError{status: grpcStatus.New(grpcCodes.Canceled, "")},
			codes: []grpcCodes.Code{grpcCodes.Canceled},
			match: true,
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   fmt.Errorf("wrapped: %w", &transportError{status: grpcStatus.New(grpcCodes.Canceled, "")}),
			codes: []grpcCodes.Code{grpcCodes.Canceled},
			match: true,
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   fmt.Errorf("wrapped: %w", grpcStatus.Error(grpcCodes.Canceled, "")),
			codes: []grpcCodes.Code{grpcCodes.Canceled},
			match: true,
		},
		{
			name: xtest.CurrentFileLine(),
			err: Join(
				fmt.Errorf("test"),
				grpcStatus.Error(grpcCodes.Canceled, ""),
				Retryable(fmt.Errorf("test")),
			),
			codes: []grpcCodes.Code{grpcCodes.Canceled},
			match: true,
		},
		// no match grpc status code
		{
			name:  xtest.CurrentFileLine(),
			err:   grpcStatus.Error(grpcCodes.Canceled, ""),
			codes: []grpcCodes.Code{grpcCodes.Aborted},
			match: false,
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   &transportError{status: grpcStatus.New(grpcCodes.Canceled, "")},
			codes: []grpcCodes.Code{grpcCodes.Aborted},
			match: false,
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   fmt.Errorf("wrapped: %w", &transportError{status: grpcStatus.New(grpcCodes.Canceled, "")}),
			codes: []grpcCodes.Code{grpcCodes.Aborted},
			match: false,
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   fmt.Errorf("wrapped: %w", grpcStatus.Error(grpcCodes.Canceled, "")),
			codes: []grpcCodes.Code{grpcCodes.Aborted},
			match: false,
		},
		{
			name: xtest.CurrentFileLine(),
			err: Join(
				fmt.Errorf("test"),
				grpcStatus.Error(grpcCodes.Canceled, ""),
				Retryable(fmt.Errorf("test")),
			),
			codes: []grpcCodes.Code{grpcCodes.Aborted},
			match: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.match, IsTransportError(tt.err, tt.codes...))
		})
	}
}

func TestGrpcError(t *testing.T) {
	for _, err := range []error{
		WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, "")),
		WithStackTrace(WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, ""))),
		WithStackTrace(WithStackTrace(WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, "")))),
		WithStackTrace(Transport(grpcStatus.Error(grpcCodes.Aborted, ""))),
		WithStackTrace(Transport(WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, "")))),
		WithStackTrace(Transport(WithStackTrace(WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, ""))))),
	} {
		t.Run(err.Error(), func(t *testing.T) {
			require.True(t, IsTransportError(err))
			s, has := grpcStatus.FromError(err)
			require.True(t, has)
			require.NotNil(t, s)
		})
	}
}

func Test_transportError_Error(t *testing.T) {
	for _, tt := range []struct {
		err  error
		text string
	}{
		{
			err:  Transport(grpcStatus.Error(grpcCodes.FailedPrecondition, "")),
			text: "transport/FailedPrecondition (code = 9, source error = \"rpc error: code = FailedPrecondition desc = \")",
		},
		{
			err:  Transport(grpcStatus.Error(grpcCodes.Unavailable, ""), WithAddress("localhost:2135")),
			text: "transport/Unavailable (code = 14, source error = \"rpc error: code = Unavailable desc = \", address: \"localhost:2135\")", //nolint:lll
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.text, tt.err.Error())
		})
	}
}

func TestTransportErrorName(t *testing.T) {
	for _, tt := range []struct {
		err  error
		name string
	}{
		{
			err:  nil,
			name: "",
		},
		{
			err:  grpcStatus.Error(grpcCodes.Aborted, ""),
			name: "transport/Aborted",
		},
		{
			err:  TransportError(grpcStatus.Error(grpcCodes.Aborted, "")),
			name: "transport/Aborted",
		},
		{
			err:  WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, "")),
			name: "transport/Aborted",
		},
		{
			err:  WithStackTrace(TransportError(grpcStatus.Error(grpcCodes.Aborted, ""))),
			name: "transport/Aborted",
		},
	} {
		t.Run("", func(t *testing.T) {
			if tt.err == nil {
				require.Nil(t, TransportError(tt.err)) //nolint:testifylint
			} else {
				require.Equal(t, tt.name, TransportError(tt.err).Name())
			}
		})
	}
}

func TestMustBanConn(t *testing.T) {
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
			pessimize := MustBanConn(test.error)
			if pessimize != test.pessimize {
				t.Errorf("unexpected pessimization status for error `%v`: %t, exp: %t", test.error, pessimize, test.pessimize)
			}
		})
	}
}
