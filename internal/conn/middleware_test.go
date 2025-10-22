package conn

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func TestMiddleware_WithContextModifier(t *testing.T) {
	t.Run("ModifyContextInInvoke", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := NewMockClientConnInterface(ctrl)
		var capturedCtx context.Context
		cc.EXPECT().Invoke(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
				capturedCtx = ctx

				return nil
			})

		modifiedCtx := WithContextModifier(cc, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, "test-key", "test-value") //nolint:revive,staticcheck
		})

		err := modifiedCtx.Invoke(context.Background(), "/test", nil, nil)
		require.NoError(t, err)
		require.NotNil(t, capturedCtx)
		require.Equal(t, "test-value", capturedCtx.Value("test-key"))
	})

	t.Run("ModifyContextInNewStream", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := NewMockClientConnInterface(ctrl)
		var capturedCtx context.Context
		cc.EXPECT().NewStream(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				capturedCtx = ctx

				return nil, fmt.Errorf("test error")
			})

		modifiedCtx := WithContextModifier(cc, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, "stream-key", "stream-value") //nolint:revive,staticcheck
		})

		_, err := modifiedCtx.NewStream(context.Background(), &grpc.StreamDesc{}, "/test")
		require.Error(t, err)
		require.NotNil(t, capturedCtx)
		require.Equal(t, "stream-value", capturedCtx.Value("stream-key"))
	})
}

func TestMiddleware_WithAppendOptions(t *testing.T) {
	t.Run("AppendOptionsToInvoke", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := NewMockClientConnInterface(ctrl)
		var capturedOpts []grpc.CallOption
		cc.EXPECT().Invoke(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
				capturedOpts = opts

				return nil
			})

		testOpt := grpc.EmptyCallOption{}
		modifiedCtx := WithAppendOptions(cc, testOpt)

		err := modifiedCtx.Invoke(context.Background(), "/test", nil, nil)
		require.NoError(t, err)
		require.Len(t, capturedOpts, 1)
	})

	t.Run("AppendOptionsToNewStream", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := NewMockClientConnInterface(ctrl)

		var capturedOpts []grpc.CallOption
		cc.EXPECT().NewStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				capturedOpts = opts

				return nil, fmt.Errorf("test error")
			})

		testOpt := grpc.EmptyCallOption{}
		modifiedCtx := WithAppendOptions(cc, testOpt)

		_, err := modifiedCtx.NewStream(context.Background(), &grpc.StreamDesc{}, "/test")
		require.Error(t, err)
		require.Len(t, capturedOpts, 1)
	})
}

func TestMiddleware_WithBeforeFunc(t *testing.T) {
	t.Run("CallBeforeFuncInInvoke", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := NewMockClientConnInterface(ctrl)

		called := false
		cc.EXPECT().Invoke(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		modifiedCtx := WithBeforeFunc(cc, func() {
			called = true
		})

		err := modifiedCtx.Invoke(context.Background(), "/test", nil, nil)
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("CallBeforeFuncInNewStream", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := NewMockClientConnInterface(ctrl)

		called := false
		cc.EXPECT().NewStream(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("test error"))

		modifiedCtx := WithBeforeFunc(cc, func() {
			called = true
		})

		_, err := modifiedCtx.NewStream(context.Background(), &grpc.StreamDesc{}, "/test")
		require.Error(t, err)
		require.True(t, called)
	})
}

func TestMiddleware_Chaining(t *testing.T) {
	t.Run("ChainMultipleMiddlewares", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := NewMockClientConnInterface(ctrl)

		var capturedCtx context.Context
		beforeCalled := false

		cc.EXPECT().Invoke(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
				capturedCtx = ctx

				return nil
			})

		testOpt := grpc.EmptyCallOption{}
		wrapped := WithBeforeFunc(cc, func() {
			beforeCalled = true
		})
		wrapped = WithAppendOptions(wrapped, testOpt)
		wrapped = WithContextModifier(wrapped, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, "chain-key", "chain-value") //nolint:revive,staticcheck
		})

		err := wrapped.Invoke(context.Background(), "/test", nil, nil)
		require.NoError(t, err)
		require.True(t, beforeCalled)
		require.NotNil(t, capturedCtx)
		require.Equal(t, "chain-value", capturedCtx.Value("chain-key"))
	})
}
