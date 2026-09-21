package partition

import (
	"context"
	"slices"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// topicDescribeCall records the topic path of one Describe invocation.
type topicDescribeCall struct {
	Path string
}

// mockTopicDescriber records calls and returns its configured description unless the context is canceled.
type mockTopicDescriber struct {
	mu          sync.Mutex
	calls       []topicDescribeCall
	description topictypes.TopicDescription
}

// doneObservedContext signals when the code under test starts observing context cancellation.
type doneObservedContext struct {
	context.Context //nolint:containedctx // Test wrapper around the observed context.

	once     sync.Once
	observed chan struct{}
}

func (c *doneObservedContext) Done() <-chan struct{} {
	c.once.Do(func() {
		close(c.observed)
	})

	return c.Context.Done()
}

func (m *mockTopicDescriber) Describe(ctx context.Context, path string) (topictypes.TopicDescription, error) {
	m.mu.Lock()
	m.calls = append(m.calls, topicDescribeCall{Path: path})
	m.mu.Unlock()

	return m.description, ctx.Err()
}

func (m *mockTopicDescriber) Calls() []topicDescribeCall {
	m.mu.Lock()
	defer m.mu.Unlock()

	return slices.Clone(m.calls)
}
