package partition_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/partition"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestPartitionsByPartitionIDReturnsPartition(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 42}},
	}}
	partitions, _ := partition.NewSources(describer.Describe).Get("test/topic").Partitions(t.Context())

	assert.Equal(t, int64(42), partitions.ByPartitionID(42).ID())
}

func TestPartitionsByPartitionIDReturnsInactiveMissingPartition(t *testing.T) {
	partitions, _ := partition.NewSources((&mockTopicDescriber{}).Describe).Get("test/topic").Partitions(t.Context())

	partition := partitions.ByPartitionID(42)

	assert.Equal(t, int64(42), partition.ID())
	assert.False(t, partition.IsActive())
}

func TestPartitionsCanBeReadConcurrently(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{
			{PartitionID: 1},
			{PartitionID: 2, ParentPartitionIDs: []int64{1}},
		},
	}}
	partitions, _ := partition.NewSources(describer.Describe).Get("test/topic").Partitions(t.Context())

	var wg sync.WaitGroup
	wg.Add(1000)
	for range 1000 {
		go func() {
			defer wg.Done()
			assert.Equal(t, []int64{1, 2}, partitions.All().IDs())
			assert.Equal(t, []int64{1}, partitions.ByPartitionID(2).Parents().IDs())
		}()
	}
	wg.Wait()
}
