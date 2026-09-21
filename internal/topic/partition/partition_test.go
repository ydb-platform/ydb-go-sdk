package partition

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestPartitionIsActiveReturnsDescribedState(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 42, Active: true}},
	}}
	partitions, _ := NewSources(describer.Describe).Get("test/topic").Partitions(t.Context())

	assert.True(t, partitions.ByPartitionID(42).IsActive())
}

func TestPartitionIDReturnsDescribedID(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 42}},
	}}
	partitions, _ := NewSources(describer.Describe).Get("test/topic").Partitions(t.Context())

	assert.Equal(t, int64(42), partitions.All()[0].ID())
}

func TestPartitionParents(t *testing.T) {
	tests := []struct {
		name       string
		partitions []topictypes.PartitionInfo
		partition  int64
		want       []int64
	}{
		{
			name:       "no parents",
			partitions: []topictypes.PartitionInfo{{PartitionID: 1}},
			partition:  1,
			want:       []int64{},
		},
		{
			name: "direct parent",
			partitions: []topictypes.PartitionInfo{
				{PartitionID: 1},
				{PartitionID: 2, ParentPartitionIDs: []int64{1}},
			},
			partition: 2,
			want:      []int64{1},
		},
		{
			name: "parent chain",
			partitions: []topictypes.PartitionInfo{
				{PartitionID: 0},
				{PartitionID: 1, ParentPartitionIDs: []int64{0}},
				{PartitionID: 2, ParentPartitionIDs: []int64{1}},
			},
			partition: 2,
			want:      []int64{1, 0},
		},
		{
			name: "multiple branches",
			partitions: []topictypes.PartitionInfo{
				{PartitionID: 1},
				{PartitionID: 2, ParentPartitionIDs: []int64{1}},
				{PartitionID: 3},
				{PartitionID: 4, ParentPartitionIDs: []int64{2, 3}},
			},
			partition: 4,
			want:      []int64{2, 3, 1},
		},
		{
			name: "shared ancestors occur once",
			partitions: []topictypes.PartitionInfo{
				{PartitionID: 0},
				{PartitionID: 1, ParentPartitionIDs: []int64{0}},
				{PartitionID: 2, ParentPartitionIDs: []int64{1}},
				{PartitionID: 3, ParentPartitionIDs: []int64{1}},
				{PartitionID: 4, ParentPartitionIDs: []int64{2, 3}},
			},
			partition: 4,
			want:      []int64{2, 3, 1, 0},
		},
		{
			name: "duplicate parents occur once",
			partitions: []topictypes.PartitionInfo{
				{PartitionID: 1},
				{PartitionID: 2, ParentPartitionIDs: []int64{1, 1}},
			},
			partition: 2,
			want:      []int64{1},
		},
		{
			name: "cycle stops at original partition",
			partitions: []topictypes.PartitionInfo{
				{PartitionID: 1, ParentPartitionIDs: []int64{2}},
				{PartitionID: 2, ParentPartitionIDs: []int64{1}},
			},
			partition: 2,
			want:      []int64{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			describer := &mockTopicDescriber{description: topictypes.TopicDescription{Partitions: tt.partitions}}
			partitions, _ := NewSources(describer.Describe).Get("test/topic").Partitions(t.Context())

			assert.ElementsMatch(t, tt.want, partitions.ByPartitionID(tt.partition).Parents().IDs())
		})
	}
}

func TestPartitionParentsReturnsMissingParentAsInactive(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
		{PartitionID: 2, ParentPartitionIDs: []int64{99}},
	}}}
	partitions, _ := NewSources(describer.Describe).Get("test/topic").Partitions(t.Context())

	parents := partitions.ByPartitionID(2).Parents()

	require.Len(t, parents, 1)
	assert.Equal(t, int64(99), parents[0].ID())
	assert.False(t, parents[0].IsActive())
	assert.Empty(t, parents[0].Parents())
}
