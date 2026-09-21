package partition

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestRouterChoosePartitionRejectsMissingPartition(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 1, Active: true}},
	}}
	router, _ := NewSources(describer.Describe).Get("test/topic").NewRouter(
		t.Context(), &fixedChooser{partitionID: 2})

	_, err := router.ChoosePartition(topicwriterinternal.PublicMessage{})

	assert.EqualError(t, err, "partition 2 does not exist or is inactive")
}

func TestRouterChoosePartitionRejectsInactivePartition(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 2}},
	}}
	router, _ := NewSources(describer.Describe).Get("test/topic").NewRouter(
		t.Context(), &fixedChooser{partitionID: 2})

	_, err := router.ChoosePartition(topicwriterinternal.PublicMessage{})

	assert.EqualError(t, err, "partition 2 does not exist or is inactive")
}

func TestRouterChoosePartitionReturnsChooserError(t *testing.T) {
	chooseErr := errors.New("choose partition")
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 1, Active: true}},
	}}
	router, _ := NewSources(describer.Describe).Get("test/topic").NewRouter(
		t.Context(), &errorChooser{chooseErr: chooseErr})

	_, err := router.ChoosePartition(topicwriterinternal.PublicMessage{})

	assert.ErrorIs(t, err, chooseErr)
}

func TestRouterChoosePartitionUsesMessagePartitionWithoutChooser(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 42, Active: true}},
	}}
	router, _ := NewSources(describer.Describe).Get("test/topic").NewRouter(t.Context(), nil)

	partitionID, err := router.ChoosePartition(topicwriterinternal.PublicMessage{PartitionID: 42})

	require.NoError(t, err)
	assert.Equal(t, int64(42), partitionID)
}

func TestRouterStopsChoosingAfterTopologyUpdateFailure(t *testing.T) {
	updateErr := errors.New("update chooser")
	var calls atomic.Int64
	source := NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		if calls.Add(1) == 1 {
			return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
				{PartitionID: 1, Active: true},
			}}, nil
		}

		return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
			{PartitionID: 1, ChildPartitionIDs: []int64{2}},
			{PartitionID: 2, Active: true, ParentPartitionIDs: []int64{1}},
		}}, nil
	}).Get("test/topic")
	router, _ := source.NewRouter(t.Context(), &updateErrorChooser{err: updateErr})
	waitForReplacement := source.NotifySessionError(t.Context(), 1, partitionInactiveError())
	require.NoError(t, waitForReplacement(t.Context()))

	_, err := router.ChoosePartition(topicwriterinternal.PublicMessage{})

	assert.ErrorIs(t, err, updateErr)
}
