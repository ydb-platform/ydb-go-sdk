package query

import (
	"context"
)

// stubAttachStreamContext configures Context() on attach stream mocks.
// attach() uses stream.Context() for node-shutdown pessimization.
func stubAttachStreamContext(stream *MockQueryService_AttachSessionClient, ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	stream.EXPECT().Context().Return(ctx).AnyTimes()
}
