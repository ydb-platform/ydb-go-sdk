package partition_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/partition"
)

func TestNewSourcesReturnsSources(t *testing.T) {
	describer := &mockTopicDescriber{}
	sources := partition.NewSources(describer.Describe)

	assert.NotNil(t, sources)
}

func TestSourcesGetReturnsSource(t *testing.T) {
	describer := &mockTopicDescriber{}
	sources := partition.NewSources(describer.Describe)

	assert.NotNil(t, sources.Get("test/topic"))
}

func TestSourcesGetDoesNotDescribeTopic(t *testing.T) {
	describer := &mockTopicDescriber{}
	sources := partition.NewSources(describer.Describe)

	_ = sources.Get("test/topic")

	assert.Empty(t, describer.Calls())
}

func TestSourcesGetKeepsTopicCachesIndependent(t *testing.T) {
	describer := &mockTopicDescriber{}
	sources := partition.NewSources(describer.Describe)

	_, _ = sources.Get("test/topic-1").Partitions(t.Context())
	_, _ = sources.Get("test/topic-2").Partitions(t.Context())

	calls := describer.Calls()
	require.Len(t, calls, 2)
	assert.Equal(t, "test/topic-1", calls[0].Path)
	assert.Equal(t, "test/topic-2", calls[1].Path)
}

func TestSourcesGetSharesTopicCache(t *testing.T) {
	describer := &mockTopicDescriber{}
	sources := partition.NewSources(describer.Describe)

	_, _ = sources.Get("test/topic").NewRouter(t.Context(), nil)
	_, _ = sources.Get("test/topic").NewRouter(t.Context(), nil)

	assert.Len(t, describer.Calls(), 1)
}

func TestSourcesGetDescribesTopicOnceForConcurrentRouters(t *testing.T) {
	ctx := t.Context()
	describer := &mockTopicDescriber{}
	sources := partition.NewSources(describer.Describe)

	start := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1000)
	for range 1000 {
		go func() {
			defer wg.Done()
			<-start
			_, _ = sources.Get("test/topic").NewRouter(ctx, nil)
		}()
	}
	close(start)
	wg.Wait()

	assert.Len(t, describer.Calls(), 1)
}
