package stubs

import (
	"context"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type stubTopicClient struct {
	describe func(ctx context.Context, path string) (topictypes.TopicDescription, error)
}

func NewStubTopicClient(t testing.TB, desc topictypes.TopicDescription) *stubTopicClient {
	t.Helper()

	return &stubTopicClient{
		describe: func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return desc, nil
		},
	}
}

func NewStubTopicClientWithError(t testing.TB, describeErr error) *stubTopicClient {
	t.Helper()

	return &stubTopicClient{
		describe: func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return topictypes.TopicDescription{}, describeErr
		},
	}
}

// NewStubTopicClientWithSplits creates a topic client whose Describe returns the current
// description from state. When state.RecordSplit(partitionID) is called (e.g. by the
// writer stub on OVERLOADED), the next Describe returns the partition with ChildPartitionIDs
// and two new partitions with bounds [from, mid) and [mid, to).
func NewStubTopicClientWithSplits(t testing.TB, state *DescribeWithSplitsState) *stubTopicClient {
	t.Helper()

	return &stubTopicClient{
		describe: func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return state.GetDescription(), nil
		},
	}
}

func (m *stubTopicClient) Describe(ctx context.Context, path string) (topictypes.TopicDescription, error) {
	return m.describe(ctx, path)
}
