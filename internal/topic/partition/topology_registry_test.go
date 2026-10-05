package partition_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/partition"
)

func TestNewTopologyRegistryReturnsRegistry(t *testing.T) {
	describer := &mockTopicDescriber{}
	registry := partition.NewTopologyRegistry(describer.Describe)

	assert.NotNil(t, registry)
}

func TestTopologyRegistryGetReturnsTopology(t *testing.T) {
	describer := &mockTopicDescriber{}
	registry := partition.NewTopologyRegistry(describer.Describe)

	assert.NotNil(t, registry.Get("test/topic"))
}

func TestTopologyRegistryGetDoesNotDescribeTopic(t *testing.T) {
	describer := &mockTopicDescriber{}
	registry := partition.NewTopologyRegistry(describer.Describe)

	_ = registry.Get("test/topic")

	assert.Empty(t, describer.Calls())
}

func TestTopologyRegistryGetKeepsTopicCachesIndependent(t *testing.T) {
	describer := &mockTopicDescriber{}
	registry := partition.NewTopologyRegistry(describer.Describe)

	_, _ = registry.Get("test/topic-1").Partitions(t.Context())
	_, _ = registry.Get("test/topic-2").Partitions(t.Context())

	calls := describer.Calls()
	require.Len(t, calls, 2)
	assert.Equal(t, "test/topic-1", calls[0].Path)
	assert.Equal(t, "test/topic-2", calls[1].Path)
}

func TestTopologyRegistryGetSharesTopicCache(t *testing.T) {
	describer := &mockTopicDescriber{}
	registry := partition.NewTopologyRegistry(describer.Describe)

	_, _ = registry.Get("test/topic").Partitions(t.Context())
	_, _ = registry.Get("test/topic").Partitions(t.Context())

	assert.Len(t, describer.Calls(), 1)
}

func TestTopologyRegistryGetDescribesTopicOnceForConcurrentSnapshots(t *testing.T) {
	ctx := t.Context()
	describer := &mockTopicDescriber{}
	registry := partition.NewTopologyRegistry(describer.Describe)

	start := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1000)
	for range 1000 {
		go func() {
			defer wg.Done()
			<-start
			_, _ = registry.Get("test/topic").Partitions(ctx)
		}()
	}
	close(start)
	wg.Wait()

	assert.Len(t, describer.Calls(), 1)
}
