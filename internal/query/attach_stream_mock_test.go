package query

import (
	"context"
)

// stubAttachStreamContext configures Context() on attach stream mocks with background context.
// attach() uses stream.Context() for node-shutdown pessimization.
func stubAttachStreamContext(stream *MockQueryService_AttachSessionClient) {
	stream.EXPECT().Context().Return(context.Background()).AnyTimes()
}

// stubAttachStreamContextWith configures Context() on attach stream mocks.
func stubAttachStreamContextWith(ctx context.Context, stream *MockQueryService_AttachSessionClient) {
	stream.EXPECT().Context().Return(ctx).AnyTimes()
}
