package xerrors

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
)

func TestJoin(t *testing.T) {
	for _, tt := range []struct {
		join error
		iss  []error
		ass  []any
		s    string
	}{
		{
			join: Join(context.Canceled),
			iss:  []error{context.Canceled},
			ass:  nil,
			s:    "context canceled",
		},
		{
			join: Join(context.Canceled, context.DeadlineExceeded, Operation()),
			iss:  []error{context.Canceled, context.DeadlineExceeded},
			ass: []any{func() any {
				var i isYdbError

				return &i
			}()},
			s: "[\"context canceled\",\"context deadline exceeded\",\"operation/STATUS_CODE_UNSPECIFIED (code = 0)\"]",
		},
		{
			join: Join(context.Canceled, context.DeadlineExceeded, nil),
			iss:  []error{context.Canceled, context.DeadlineExceeded},
			s:    "[\"context canceled\",\"context deadline exceeded\"]",
		},
		{
			join: Join(nil, context.DeadlineExceeded, nil),
			iss:  []error{context.DeadlineExceeded},
			s:    "context deadline exceeded",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.s, tt.join.Error())
			if len(tt.iss) > 0 {
				require.True(t, Is(tt.join, tt.iss...))
			}
			if len(tt.ass) > 0 {
				require.True(t, As(tt.join, tt.ass...))
			}
		})
	}
}

func TestUnwrapJoined(t *testing.T) {
	err1 := context.Canceled
	err2 := context.DeadlineExceeded

	joined := Join(err1, err2)

	unwrappable := joined.(interface{ Unwrap() []error })
	inners := unwrappable.Unwrap()
	assert.Contains(t, inners, err1)
	assert.Contains(t, inners, err2)
}

func TestJoinAsYDBError(t *testing.T) {
	for _, joined := range []error{
		Join(context.Canceled, Transport(grpcStatus.Error(grpcCodes.Canceled, "test"))),
		Join(Transport(grpcStatus.Error(grpcCodes.Canceled, "test")), context.Canceled),
		WithStackTrace(Join(context.Canceled, Transport(grpcStatus.Error(grpcCodes.Canceled, "test")))),
		WithStackTrace(Join(Transport(grpcStatus.Error(grpcCodes.Canceled, "test")), context.Canceled)),
		func() error {
			ctxGuard := xcontext.NewCancelsGuard()
			defer ctxGuard.Cancel()
			ctx, cancel := ctxGuard.WithCancel(t.Context())
			cancel()

			return Join(Transport(grpcStatus.Error(grpcCodes.Canceled, "test")), ctx.Err())
		}(),
		func() error {
			ctxGuard := xcontext.NewCancelsGuard()
			defer ctxGuard.Cancel()
			ctx, cancel := ctxGuard.WithCancel(t.Context())
			cancel()

			return WithStackTrace(Join(Transport(grpcStatus.Error(grpcCodes.Canceled, "test")), ctx.Err()))
		}(),
		func() error {
			ctxGuard := xcontext.NewCancelsGuard()
			defer ctxGuard.Cancel()
			ctx, cancel := ctxGuard.WithCancel(t.Context())
			cancel()

			return Join(ctx.Err(), Transport(grpcStatus.Error(grpcCodes.Canceled, "test")))
		}(),
		func() error {
			ctxGuard := xcontext.NewCancelsGuard()
			defer ctxGuard.Cancel()
			ctx, cancel := ctxGuard.WithCancel(t.Context())
			cancel()

			return WithStackTrace(Join(ctx.Err(), Transport(grpcStatus.Error(grpcCodes.Canceled, "test"))))
		}(),
	} {
		t.Run(joined.Error(), func(t *testing.T) {
			var ydbErr Error
			require.True(t, As(joined, &ydbErr))
			require.EqualValues(t, grpcCodes.Canceled, ydbErr.Code())
		})
	}
}
