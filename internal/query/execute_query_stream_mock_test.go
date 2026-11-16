package query

import (
	"context"

	"go.uber.org/mock/gomock"
)

// stubExecuteQueryStreamContext configures Context() on ExecuteQuery stream mocks.
func stubExecuteQueryStreamContext(ctx context.Context, stream *MockQueryService_ExecuteQueryClient) {
	stream.EXPECT().Context().Return(ctx).AnyTimes()
}

// newExecuteQueryStreamMock returns a gomock stream with Context() stubbed for Close().
func newExecuteQueryStreamMock(ctrl *gomock.Controller) *MockQueryService_ExecuteQueryClient {
	stream := NewMockQueryService_ExecuteQueryClient(ctrl)
	stubExecuteQueryStreamContext(context.Background(), stream)

	return stream
}

// executeQueryStreamContextWithOnClose wires stream.Context() to executeCtx cancelled by onClose.
func executeQueryStreamContextWithOnClose(
	stream *MockQueryService_ExecuteQueryClient,
	opts ...resultOption,
) (context.Context, []resultOption) {
	executeCtx, executeCancel := context.WithCancel(context.Background())
	stubExecuteQueryStreamContext(executeCtx, stream)

	return executeCtx, append(opts, withStreamResultOnClose(executeCancel))
}
