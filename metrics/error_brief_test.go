package metrics

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestErrorBrief(t *testing.T) {
	for _, tt := range []struct {
		name  string
		err   error
		brief string
	}{
		{
			name:  xtest.CurrentFileLine(),
			err:   nil,
			brief: "OK",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   context.Canceled,
			brief: "context/Canceled",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   xerrors.WithStackTrace(context.Canceled),
			brief: "context/Canceled",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   context.DeadlineExceeded,
			brief: "context/DeadlineExceeded",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   xerrors.WithStackTrace(context.DeadlineExceeded),
			brief: "context/DeadlineExceeded",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   fmt.Errorf("test"),
			brief: "unknown",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   io.EOF,
			brief: "io/EOF",
		},
		{
			name: xtest.CurrentFileLine(),
			err: &net.OpError{
				Op: "write",
				Addr: &net.TCPAddr{
					IP:   []byte{0, 0, 0, 0},
					Port: 2135,
				},
				Err: grpcStatus.Error(grpcCodes.Unavailable, ""),
			},
			brief: "network/write[0.0.0.0:2135](transport/Unavailable)",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   xerrors.Retryable(fmt.Errorf("test")),
			brief: "retryable/CUSTOM",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   xerrors.Retryable(fmt.Errorf("test"), xerrors.WithName("SomeName")),
			brief: "retryable/SomeName",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   xerrors.WithStackTrace(xerrors.Retryable(fmt.Errorf("test"))),
			brief: "retryable/CUSTOM",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.WithStackTrace(
				xerrors.Retryable(fmt.Errorf("test"), xerrors.WithName("SomeName")),
			),
			brief: "retryable/SomeName",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.WithStackTrace(&net.OpError{
				Op: "write",
				Addr: &net.TCPAddr{
					IP:   []byte{0, 0, 0, 0},
					Port: 2135,
				},
				Err: grpcStatus.Error(grpcCodes.Unavailable, ""),
			}),
			brief: "network/write[0.0.0.0:2135](transport/Unavailable)",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   grpcStatus.Error(grpcCodes.Unavailable, ""),
			brief: "transport/Unavailable",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Transport(
				grpcStatus.Error(grpcCodes.Unavailable, ""),
			),
			brief: "transport/Unavailable",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
			),
			brief: "operation/BAD_REQUEST",
		},
		// errors with stack trace
		{
			name:  xtest.CurrentFileLine(),
			err:   xerrors.WithStackTrace(fmt.Errorf("test")),
			brief: "unknown",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   xerrors.WithStackTrace(io.EOF),
			brief: "io/EOF",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.WithStackTrace(
				grpcStatus.Error(grpcCodes.Unavailable, ""),
			),
			brief: "transport/Unavailable",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.WithStackTrace(xerrors.Transport(
				grpcStatus.Error(grpcCodes.Unavailable, ""),
			)),
			brief: "transport/Unavailable",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.WithStackTrace(xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
			)),
			brief: "operation/BAD_REQUEST",
		},
		// joined errors
		{
			name:  xtest.CurrentFileLine(),
			err:   xerrors.Join(fmt.Errorf("test")),
			brief: "unknown",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Join(
				fmt.Errorf("test"),
				xerrors.Retryable(fmt.Errorf("test")),
			),
			brief: "retryable/CUSTOM",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Join(
				fmt.Errorf("test"),
				xerrors.Retryable(fmt.Errorf("test"), xerrors.WithName("SomeName")),
			),
			brief: "retryable/SomeName",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Join(
				fmt.Errorf("test"),
				xerrors.Retryable(fmt.Errorf("test"), xerrors.WithName("SomeName")),
				grpcStatus.Error(grpcCodes.Unavailable, "test"),
			),
			brief: "transport/Unavailable",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   xerrors.Join(io.EOF),
			brief: "io/EOF",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Join(
				grpcStatus.Error(grpcCodes.Unavailable, ""),
			),
			brief: "transport/Unavailable",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Join(xerrors.Transport(
				grpcStatus.Error(grpcCodes.Unavailable, ""),
			)),
			brief: "transport/Unavailable",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Join(xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
			)),
			brief: "operation/BAD_REQUEST",
		},
		// joined errors with stack trace
		{
			name:  xtest.CurrentFileLine(),
			err:   xerrors.Join(xerrors.WithStackTrace(fmt.Errorf("test"))),
			brief: "unknown",
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   xerrors.Join(xerrors.WithStackTrace(io.EOF)),
			brief: "io/EOF",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Join(xerrors.WithStackTrace(xerrors.Transport(
				grpcStatus.Error(grpcCodes.Unavailable, ""),
			))),
			brief: "transport/Unavailable",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Join(xerrors.WithStackTrace(xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
			))),
			brief: "operation/BAD_REQUEST",
		},
		// joined errors (mixed types)
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Join(
				xerrors.WithStackTrace(fmt.Errorf("test")),
				xerrors.WithStackTrace(io.EOF),
			),
			brief: "io/EOF",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.WithStackTrace(xerrors.Join(
				xerrors.WithStackTrace(fmt.Errorf("test")),
				xerrors.WithStackTrace(io.EOF),
			)),
			brief: "io/EOF",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Join(
				io.EOF,
				grpcStatus.Error(grpcCodes.Unavailable, ""),
				xerrors.WithStackTrace(xerrors.Operation(
					xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
				)),
			),
			brief: "io/EOF",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Join(
				&net.OpError{
					Op: "write",
					Addr: &net.TCPAddr{
						IP:   []byte{0, 0, 0, 0},
						Port: 2135,
					},
					Err: grpcStatus.Error(grpcCodes.Unavailable, ""),
				},
				io.EOF,
				grpcStatus.Error(grpcCodes.Unavailable, ""),
				xerrors.WithStackTrace(xerrors.Operation(
					xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
				)),
			),
			brief: "io/EOF",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.Join(
				grpcStatus.Error(grpcCodes.Unavailable, ""),
				xerrors.WithStackTrace(xerrors.Operation(
					xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
				)),
			),
			brief: "transport/Unavailable",
		},
		{
			name: xtest.CurrentFileLine(),
			err: xerrors.WithStackTrace(xerrors.Join(
				xerrors.WithStackTrace(xerrors.Transport(
					grpcStatus.Error(grpcCodes.Unavailable, ""),
				)),
				xerrors.WithStackTrace(xerrors.Operation(
					xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
				)),
			)),
			brief: "transport/Unavailable",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.brief, errorBrief(tt.err))
		})
	}
}
