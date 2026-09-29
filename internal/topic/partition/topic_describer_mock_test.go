package partition_test

import (
	"context"
	"slices"
	"sync"
	"time"

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
	deadline    time.Time
	hasDeadline bool
	done        <-chan struct{}
	err         func() error
	value       func(any) any
	once        sync.Once
	observed    chan struct{}
}

func newDoneObservedContext(parent context.Context, observed chan struct{}) *doneObservedContext {
	deadline, hasDeadline := parent.Deadline()

	return &doneObservedContext{
		deadline:    deadline,
		hasDeadline: hasDeadline,
		done:        parent.Done(),
		err:         parent.Err,
		value:       parent.Value,
		observed:    observed,
	}
}

func (c *doneObservedContext) Deadline() (time.Time, bool) {
	return c.deadline, c.hasDeadline
}

func (c *doneObservedContext) Done() <-chan struct{} {
	c.once.Do(func() {
		close(c.observed)
	})

	return c.done
}

func (c *doneObservedContext) Err() error {
	return c.err()
}

func (c *doneObservedContext) Value(key any) any {
	return c.value(key)
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
