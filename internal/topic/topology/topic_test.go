package topology_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topology"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestTopicPartitionsReturnsPartitions(t *testing.T) {
	describer := &mockTopicDescriber{}
	topology := topology.NewRegistry(describer.Describe).Get("test/topic")

	partitions, err := topology.Partitions(t.Context())

	require.NoError(t, err)
	assert.NotNil(t, partitions)
}

func TestTopicPartitionsReturnsDescribeError(t *testing.T) {
	describeErr := errors.New("describe topic failed")
	topology := topology.NewRegistry(func(context.Context, string) (topictypes.TopicDescription, error) {
		return topictypes.TopicDescription{}, describeErr
	}).Get("test/topic")

	_, err := topology.Partitions(t.Context())

	assert.ErrorIs(t, err, describeErr)
}

func TestTopicPartitionsRetriesRetryableDescribeError(t *testing.T) {
	describeErr := retry.RetryableError(
		errors.New("describe topic failed"),
		retry.WithBackoff(retry.TypeNoBackoff),
	)
	topology := newTopicWithDescribeResults(
		topicDescribeResult{err: describeErr},
		topicDescribeResult{description: topicWithActivePartitions(42)},
	)

	partitions, err := topology.Partitions(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []int64{42}, partitions.All().IDs())
}

func TestTopicPartitionsDescribesRequestedTopic(t *testing.T) {
	topicName := "test/topic"
	describer := &mockTopicDescriber{}
	topology := topology.NewRegistry(describer.Describe).Get(topicName)

	_, _ = topology.Partitions(t.Context())

	assert.Equal(t, topicName, describer.Calls()[0].Path)
}

func TestTopicPartitionsReturnsDescribedPartitionIDs(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 42, Active: true}},
	}}
	topology := topology.NewRegistry(describer.Describe).Get("test/topic")

	partitions, err := topology.Partitions(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []int64{42}, partitions.All().IDs())
}

func TestTopicPartitionsDescribesTopicOnce(t *testing.T) {
	describer := &mockTopicDescriber{}
	topology := topology.NewRegistry(describer.Describe).Get("test/topic")

	_, _ = topology.Partitions(t.Context())
	_, _ = topology.Partitions(t.Context())

	assert.Len(t, describer.Calls(), 1)
}

func TestTopicTopicDescriptionUsesCachedPartitions(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{
			PartitionID: 1, Active: true, ChildPartitionIDs: []int64{2},
		}},
	}}
	topology := topology.NewRegistry(describer.Describe).Get("test/topic")

	first, err := topology.TopicDescription(t.Context())
	require.NoError(t, err)
	first.Partitions[0].ChildPartitionIDs[0] = 99
	second, err := topology.TopicDescription(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []int64{2}, second.Partitions[0].ChildPartitionIDs)
	assert.Len(t, describer.Calls(), 1)
}

func TestTopicPartitionsDescribesTopicOnceConcurrently(t *testing.T) {
	ctx := t.Context()
	describer := &mockTopicDescriber{}
	topology := topology.NewRegistry(describer.Describe).Get("test/topic")
	start := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1000)
	for range 1000 {
		go func() {
			defer wg.Done()
			<-start
			_, _ = topology.Partitions(ctx)
		}()
	}
	close(start)
	wg.Wait()

	assert.Len(t, describer.Calls(), 1)
}

func TestTopicPartitionsRetriesWhenConcurrentLoadContextIsCanceled(t *testing.T) {
	var calls atomic.Int64
	describeStarted := make(chan struct{})
	topology := topology.NewRegistry(func(ctx context.Context, _ string) (topictypes.TopicDescription, error) {
		if calls.Add(1) == 1 {
			close(describeStarted)
			<-ctx.Done()

			return topictypes.TopicDescription{}, ctx.Err()
		}

		return topicWithActivePartitions(42), nil
	}).Get("test/topic")
	loadCtx, cancelLoad := context.WithCancel(t.Context())
	loadResult := startLoadingPartitions(loadCtx, topology)
	<-describeStarted
	waitResult := startWaitingForPartitions(t, topology)
	cancelLoad()
	require.ErrorIs(t, (<-loadResult).err, context.Canceled)

	result := <-waitResult

	require.NoError(t, result.err)
	assert.Equal(t, []int64{42}, result.partitions.All().IDs())
}

func TestTopicPartitionsReturnsConcurrentDescribeError(t *testing.T) {
	describeErr := errors.New("describe topic failed")
	describeStarted := make(chan struct{})
	releaseDescribe := make(chan struct{})
	topology := topology.NewRegistry(func(context.Context, string) (topictypes.TopicDescription, error) {
		close(describeStarted)
		<-releaseDescribe

		return topictypes.TopicDescription{}, describeErr
	}).Get("test/topic")
	firstResult := startLoadingPartitions(t.Context(), topology)
	<-describeStarted
	secondResult := startWaitingForPartitions(t, topology)

	close(releaseDescribe)

	assert.ErrorIs(t, (<-firstResult).err, describeErr)
	assert.ErrorIs(t, (<-secondResult).err, describeErr)
}

func TestTopicPartitionsReloadsWhenInvalidatedDuringDescribe(t *testing.T) {
	var calls atomic.Int64
	describeStarted := make(chan struct{})
	releaseDescribe := make(chan struct{})
	topology := topology.NewRegistry(func(context.Context, string) (topictypes.TopicDescription, error) {
		partitionID := calls.Add(1)
		if partitionID == 1 {
			close(describeStarted)
			<-releaseDescribe
		}

		return topicWithActivePartitions(partitionID), nil
	}).Get("test/topic")
	result := startLoadingPartitions(t.Context(), topology)
	<-describeStarted

	topology.Invalidate()
	close(releaseDescribe)

	loaded := <-result
	require.NoError(t, loaded.err)
	assert.Equal(t, []int64{2}, loaded.partitions.All().IDs())
}

func TestTopicPartitionsReloadsAfterInvalidate(t *testing.T) {
	topology := newTopicWithDescriptions(
		topicWithActivePartitions(1),
		topicWithActivePartitions(2),
	)
	_, _ = topology.Partitions(t.Context())

	topology.Invalidate()
	partitions, err := topology.Partitions(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []int64{2}, partitions.All().IDs())
}

func TestTopicReportInactivePartitionReloads(t *testing.T) {
	topic := newTopicWithDescriptions(
		topicWithActivePartitions(1),
		topicAfterReplacement(1, 2),
	)
	_, err := topic.Partitions(t.Context())
	require.NoError(t, err)

	topic.ReportInactivePartition(1)
	partitions, err := topic.Partitions(t.Context())

	require.NoError(t, err)
	assert.True(t, partitions.ByPartitionID(2).IsActive())
}

func TestTopicPartitionsWaitsForPublishedReplacement(t *testing.T) {
	topology := newTopicWithDescriptions(
		topicWithActivePartitions(1),
		topicWithActivePartitions(1),
		topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
			{PartitionID: 1, ChildPartitionIDs: []int64{2}},
		}},
		topicAfterReplacement(1, 2),
	)
	_, err := topology.Partitions(t.Context())
	require.NoError(t, err)
	topology.ReportInactivePartition(1)

	partitions, err := topology.Partitions(t.Context())

	require.NoError(t, err)
	assert.False(t, partitions.ByPartitionID(1).IsActive())
	assert.True(t, partitions.ByPartitionID(2).IsActive())
}

func TestTopicPartitionsPublishesReplacementReportedInactiveDuringDescribe(t *testing.T) {
	unexpectedDescribe := errors.New("unexpected extra describe")
	var (
		topic *topology.Topic
		calls atomic.Int64
	)
	topic = topology.NewRegistry(func(context.Context, string) (topictypes.TopicDescription, error) {
		switch calls.Add(1) {
		case 1:
			return topicWithActivePartitions(1), nil
		case 2:
			topic.ReportInactivePartition(1)

			return topicAfterReplacement(1, 2), nil
		default:
			return topictypes.TopicDescription{}, unexpectedDescribe
		}
	}).Get("test/topic")
	_, err := topic.Partitions(t.Context())
	require.NoError(t, err)
	topic.ReportInactivePartition(1)

	partitions, err := topic.Partitions(t.Context())

	require.NoError(t, err)
	assert.Equal(t, int64(2), calls.Load())
	assert.True(t, partitions.ByPartitionID(2).IsActive())
}

func TestTopicPartitionsWaitsForReplacementThroughInactiveDescendants(t *testing.T) {
	topology := newTopicWithDescriptions(
		topicWithActivePartitions(1),
		topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
			{PartitionID: 1, ChildPartitionIDs: []int64{2, 3}},
			{PartitionID: 2, ParentPartitionIDs: []int64{1}, ChildPartitionIDs: []int64{4, 5}},
			{PartitionID: 3, ParentPartitionIDs: []int64{1}, ChildPartitionIDs: []int64{6, 7}},
			{PartitionID: 4, Active: true, ParentPartitionIDs: []int64{2}},
			{PartitionID: 5, Active: true, ParentPartitionIDs: []int64{2}},
			{PartitionID: 6, Active: true, ParentPartitionIDs: []int64{3}},
			{PartitionID: 7, Active: true, ParentPartitionIDs: []int64{3}},
		}},
	)
	_, err := topology.Partitions(t.Context())
	require.NoError(t, err)
	topology.ReportInactivePartition(1)

	partitions, err := topology.Partitions(t.Context())

	require.NoError(t, err)
	assert.True(t, partitions.ByPartitionID(4).IsActive())
	assert.True(t, partitions.ByPartitionID(7).IsActive())
}

func TestTopicPartitionsReturnsContextErrorWhileWaitingForReplacement(t *testing.T) {
	var calls atomic.Int64
	refreshStarted := make(chan struct{})
	topology := topology.NewRegistry(func(ctx context.Context, _ string) (topictypes.TopicDescription, error) {
		if calls.Add(1) == 1 {
			return topicWithActivePartitions(1), nil
		}
		close(refreshStarted)
		<-ctx.Done()

		return topictypes.TopicDescription{}, ctx.Err()
	}).Get("test/topic")
	_, err := topology.Partitions(t.Context())
	require.NoError(t, err)
	topology.ReportInactivePartition(1)
	refreshCtx, cancelRefresh := context.WithCancel(t.Context())
	result := startLoadingPartitions(refreshCtx, topology)
	<-refreshStarted

	cancelRefresh()

	assert.ErrorIs(t, (<-result).err, context.Canceled)
}

func TestTopicPartitionsContinuesReplacementRefreshAfterDescribeError(t *testing.T) {
	describeErr := errors.New("describe topic failed")
	topology := newTopicWithDescribeResults(
		topicDescribeResult{description: topicWithActivePartitions(1)},
		topicDescribeResult{err: describeErr},
		topicDescribeResult{description: topicAfterReplacement(1, 2)},
	)
	_, err := topology.Partitions(t.Context())
	require.NoError(t, err)
	topology.ReportInactivePartition(1)

	_, err = topology.Partitions(t.Context())
	require.ErrorIs(t, err, describeErr)
	partitions, err := topology.Partitions(t.Context())

	require.NoError(t, err)
	assert.True(t, partitions.ByPartitionID(2).IsActive())
}

func TestTopicPartitionsSharesReplacementRefresh(t *testing.T) {
	var calls atomic.Int64
	releaseRefresh := make(chan struct{})
	refreshStarted := make(chan struct{})
	topology := topology.NewRegistry(func(context.Context, string) (topictypes.TopicDescription, error) {
		if calls.Add(1) == 1 {
			return topicWithActivePartitions(1), nil
		}
		close(refreshStarted)
		<-releaseRefresh

		return topicAfterReplacement(1, 2), nil
	}).Get("test/topic")
	_, err := topology.Partitions(t.Context())
	require.NoError(t, err)
	topology.ReportInactivePartition(1)
	first := startLoadingPartitions(t.Context(), topology)
	<-refreshStarted
	second := startWaitingForPartitions(t, topology)

	close(releaseRefresh)

	require.NoError(t, (<-first).err)
	require.NoError(t, (<-second).err)
	assert.Equal(t, int64(2), calls.Load())
}
