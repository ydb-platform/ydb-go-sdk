package topology_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topology"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestPartitionsByPartitionIDReturnsPartition(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 42}},
	}}
	partitions, _ := topology.NewRegistry(describer.Describe).Get("test/topic").Partitions(t.Context())

	assert.Equal(t, int64(42), partitions.ByPartitionID(42).ID())
}

func TestPartitionsByPartitionIDReturnsInactiveMissingPartition(t *testing.T) {
	describer := &mockTopicDescriber{}
	partitions, _ := topology.NewRegistry(describer.Describe).Get("test/topic").Partitions(t.Context())

	partition := partitions.ByPartitionID(42)

	assert.Equal(t, int64(42), partition.ID())
	assert.False(t, partition.IsActive())
}

func TestPartitionsInfosReturnsIndependentCopies(t *testing.T) {
	wantLastWrite := time.Unix(10, 0)
	wantMaxLag := time.Second
	lastWrite, maxLag := wantLastWrite, wantMaxLag
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{
			PartitionID:        1,
			Active:             true,
			ChildPartitionIDs:  []int64{2},
			ParentPartitionIDs: []int64{0},
			FromBound:          []byte{1},
			ToBound:            []byte{9},
			PartitionStats: topictypes.PartitionStats{
				LastWriteTime:   &lastWrite,
				MaxWriteTimeLag: &maxLag,
			},
		}},
	}}
	topic := topology.NewRegistry(describer.Describe).Get("test/topic")
	snapshot, err := topic.Partitions(t.Context())
	require.NoError(t, err)

	first := snapshot.Infos()
	first[0].ChildPartitionIDs[0] = 99
	first[0].ParentPartitionIDs[0] = 99
	first[0].FromBound[0] = 99
	first[0].ToBound[0] = 99
	*first[0].PartitionStats.LastWriteTime = time.Unix(99, 0)
	*first[0].PartitionStats.MaxWriteTimeLag = 99 * time.Second
	second := snapshot.Infos()

	require.Len(t, second, 1)
	assert.Equal(t, []int64{2}, second[0].ChildPartitionIDs)
	assert.Equal(t, []int64{0}, second[0].ParentPartitionIDs)
	assert.Equal(t, []byte{1}, second[0].FromBound)
	assert.Equal(t, []byte{9}, second[0].ToBound)
	assert.Equal(t, wantLastWrite, *second[0].PartitionStats.LastWriteTime)
	assert.Equal(t, wantMaxLag, *second[0].PartitionStats.MaxWriteTimeLag)
	assert.Len(t, describer.Calls(), 1)
}

func TestPartitionsCanBeReadConcurrently(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{
			{PartitionID: 1},
			{PartitionID: 2, ParentPartitionIDs: []int64{1}},
		},
	}}
	partitions, _ := topology.NewRegistry(describer.Describe).Get("test/topic").Partitions(t.Context())

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
