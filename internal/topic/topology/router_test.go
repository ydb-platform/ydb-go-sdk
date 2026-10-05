package topology_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topology"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestNewRouterAddsActivePartitionsToChooser(t *testing.T) {
	chooser := &recordingChooser{}
	snapshot := snapshotFromDescription(t, topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
		{PartitionID: 1, Active: true},
		{PartitionID: 2},
	}})

	router, err := topology.NewRouter(snapshot, chooser)

	require.NoError(t, err)
	assert.NotNil(t, router)
	assert.Equal(t, []int64{1}, chooser.PartitionIDs())
}

func TestNewRouterDoesNotNotifyChooserWithoutActivePartitions(t *testing.T) {
	unexpectedCallErr := errors.New("empty partition initialization")
	snapshot := snapshotFromDescription(t, topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
		{PartitionID: 1},
	}})

	router, err := topology.NewRouter(snapshot, &emptyAddErrorChooser{err: unexpectedCallErr})

	require.NoError(t, err)
	assert.NotNil(t, router)
}

func TestNewRouterReturnsChooserInitializationError(t *testing.T) {
	initializeErr := errors.New("initialize chooser")
	snapshot := snapshotFromDescription(t, topicWithActivePartitions(1))

	_, err := topology.NewRouter(snapshot, &errorChooser{addErr: initializeErr})

	assert.ErrorIs(t, err, initializeErr)
}

func TestNewRouterRejectsNilSnapshot(t *testing.T) {
	_, err := topology.NewRouter(nil, nil)

	assert.EqualError(t, err, "partitions snapshot is nil")
}

func TestRouterChoosePartitionRejectsMissingPartition(t *testing.T) {
	router, _ := topology.NewRouter(
		snapshotFromDescription(t, topicWithActivePartitions(1)),
		&fixedChooser{partitionID: 2},
	)

	_, err := router.ChoosePartition(topicwriterinternal.PublicMessage{})

	assert.EqualError(t, err, "partition 2 does not exist or is inactive")
}

func TestRouterChoosePartitionRejectsInactivePartition(t *testing.T) {
	snapshot := snapshotFromDescription(t, topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 2}},
	})
	router, _ := topology.NewRouter(snapshot, &fixedChooser{partitionID: 2})

	_, err := router.ChoosePartition(topicwriterinternal.PublicMessage{})

	assert.EqualError(t, err, "partition 2 does not exist or is inactive")
}

func TestRouterChoosePartitionReturnsChooserError(t *testing.T) {
	chooseErr := errors.New("choose partition")
	router, _ := topology.NewRouter(
		snapshotFromDescription(t, topicWithActivePartitions(1)),
		&errorChooser{chooseErr: chooseErr},
	)

	_, err := router.ChoosePartition(topicwriterinternal.PublicMessage{})

	assert.ErrorIs(t, err, chooseErr)
}

func TestRouterChoosePartitionUsesMessagePartitionWithoutChooser(t *testing.T) {
	router, _ := topology.NewRouter(snapshotFromDescription(t, topicWithActivePartitions(42)), nil)

	partitionID, err := router.ChoosePartition(topicwriterinternal.PublicMessage{PartitionID: 42})

	require.NoError(t, err)
	assert.Equal(t, int64(42), partitionID)
}

func TestRouterApplyUpdatesChooserAndSnapshot(t *testing.T) {
	chooser := &fixedChooser{partitionID: 2}
	router, err := topology.NewRouter(snapshotFromDescription(t, topicWithActivePartitions(1)), chooser)
	require.NoError(t, err)

	err = router.Apply(snapshotFromDescription(t, topicAfterReplacement(1, 2)))

	require.NoError(t, err)
	assert.Equal(t, []int64{2}, chooser.PartitionIDs())
	partitionID, err := router.ChoosePartition(topicwriterinternal.PublicMessage{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), partitionID)
}

func TestRouterApplyDoesNotNotifyChooserWithoutNewPartitions(t *testing.T) {
	unexpectedCallErr := errors.New("empty partition update")
	snapshot := snapshotFromDescription(t, topicWithActivePartitions(1))
	router, err := topology.NewRouter(snapshot, &emptyAddErrorChooser{err: unexpectedCallErr})
	require.NoError(t, err)

	err = router.Apply(snapshot)

	assert.NoError(t, err)
}

func TestRouterApplyFailureStopsChoosing(t *testing.T) {
	updateErr := errors.New("update chooser")
	router, err := topology.NewRouter(
		snapshotFromDescription(t, topicWithActivePartitions(1)),
		&updateErrorChooser{err: updateErr},
	)
	require.NoError(t, err)

	err = router.Apply(snapshotFromDescription(t, topicAfterReplacement(1, 2)))
	require.ErrorIs(t, err, updateErr)

	_, err = router.ChoosePartition(topicwriterinternal.PublicMessage{})
	assert.ErrorIs(t, err, updateErr)
}

func snapshotFromDescription(t *testing.T, description topictypes.TopicDescription) *topology.Partitions {
	t.Helper()
	topology := newTopicWithDescriptions(description)
	snapshot, err := topology.Partitions(t.Context())
	require.NoError(t, err)

	return snapshot
}
