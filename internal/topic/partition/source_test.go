package partition_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/partition"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestSourcePartitionsReturnsPartitions(t *testing.T) {
	describer := &mockTopicDescriber{}
	source := partition.NewSources(describer.Describe).Get("test/topic")

	partitions, err := source.Partitions(t.Context())

	require.NoError(t, err)
	assert.NotNil(t, partitions)
}

func TestSourcePartitionsReturnsDescribeError(t *testing.T) {
	describeErr := errors.New("describe topic failed")
	source := partition.NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		return topictypes.TopicDescription{}, describeErr
	}).Get("test/topic")

	_, err := source.Partitions(t.Context())

	assert.ErrorIs(t, err, describeErr)
}

func TestSourcePartitionsRetriesRetryableDescribeError(t *testing.T) {
	describeErr := retry.RetryableError(
		errors.New("describe topic failed"),
		retry.WithBackoff(retry.TypeNoBackoff),
	)
	source := newSourceWithDescribeResults(
		topicDescribeResult{err: describeErr},
		topicDescribeResult{description: topicWithActivePartitions(42)},
	)

	partitions, err := source.Partitions(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []int64{42}, partitions.All().IDs())
}

func TestSourcePartitionsDescribesRequestedTopic(t *testing.T) {
	topicName := "test/topic"
	describer := &mockTopicDescriber{}
	source := partition.NewSources(describer.Describe).Get(topicName)

	_, _ = source.Partitions(t.Context())

	assert.Equal(t, topicName, describer.Calls()[0].Path)
}

func TestSourcePartitionsReturnsDescribedPartitionIDs(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 42, Active: true}},
	}}
	source := partition.NewSources(describer.Describe).Get("test/topic")

	partitions, err := source.Partitions(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []int64{42}, partitions.All().IDs())
}

func TestSourcePartitionsDescribesTopicOnce(t *testing.T) {
	describer := &mockTopicDescriber{}
	source := partition.NewSources(describer.Describe).Get("test/topic")

	_, _ = source.Partitions(t.Context())
	_, _ = source.Partitions(t.Context())

	assert.Len(t, describer.Calls(), 1)
}

func TestSourcePartitionsDescribesTopicOnceConcurrently(t *testing.T) {
	ctx := t.Context()
	describer := &mockTopicDescriber{}
	source := partition.NewSources(describer.Describe).Get("test/topic")
	start := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1000)
	for range 1000 {
		go func() {
			defer wg.Done()
			<-start
			_, _ = source.Partitions(ctx)
		}()
	}
	close(start)
	wg.Wait()

	assert.Len(t, describer.Calls(), 1)
}

func TestSourcePartitionsRetriesWhenConcurrentLoadContextIsCanceled(t *testing.T) {
	var calls atomic.Int64
	describeStarted := make(chan struct{})
	source := partition.NewSources(func(ctx context.Context, _ string) (topictypes.TopicDescription, error) {
		if calls.Add(1) == 1 {
			close(describeStarted)
			<-ctx.Done()

			return topictypes.TopicDescription{}, ctx.Err()
		}

		return topicWithActivePartitions(42), nil
	}).Get("test/topic")
	loadCtx, cancelLoad := context.WithCancel(t.Context())
	loadResult := startLoadingPartitions(loadCtx, source)
	<-describeStarted
	waitResult := startWaitingForPartitions(t, source)
	cancelLoad()
	require.ErrorIs(t, (<-loadResult).err, context.Canceled)

	result := <-waitResult

	require.NoError(t, result.err)
	assert.Equal(t, []int64{42}, result.partitions.All().IDs())
}

func TestSourcePartitionsReturnsConcurrentDescribeError(t *testing.T) {
	describeErr := errors.New("describe topic failed")
	describeStarted := make(chan struct{})
	releaseDescribe := make(chan struct{})
	source := partition.NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		close(describeStarted)
		<-releaseDescribe

		return topictypes.TopicDescription{}, describeErr
	}).Get("test/topic")
	firstResult := startLoadingPartitions(t.Context(), source)
	<-describeStarted
	secondResult := startWaitingForPartitions(t, source)

	close(releaseDescribe)

	assert.ErrorIs(t, (<-firstResult).err, describeErr)
	assert.ErrorIs(t, (<-secondResult).err, describeErr)
}

func TestSourcePartitionsReloadsWhenInvalidatedDuringDescribe(t *testing.T) {
	var calls atomic.Int64
	describeStarted := make(chan struct{})
	releaseDescribe := make(chan struct{})
	source := partition.NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		partitionID := calls.Add(1)
		if partitionID == 1 {
			close(describeStarted)
			<-releaseDescribe
		}

		return topicWithActivePartitions(partitionID), nil
	}).Get("test/topic")
	result := startLoadingPartitions(t.Context(), source)
	<-describeStarted

	source.Invalidate()
	close(releaseDescribe)

	loaded := <-result
	require.NoError(t, loaded.err)
	assert.Equal(t, []int64{2}, loaded.partitions.All().IDs())
}

func TestSourcePartitionsReloadsAfterInvalidate(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicWithActivePartitions(2),
	)
	_, _ = source.Partitions(t.Context())

	source.Invalidate()
	partitions, err := source.Partitions(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []int64{2}, partitions.All().IDs())
}

func TestSourceNotifySessionErrorRejectsOverloadedWithoutPartitionInactiveIssue(t *testing.T) {
	source := partition.NewSources((&mockTopicDescriber{}).Describe).Get("test/topic")

	assert.False(t, source.NotifySessionError(1, overloadedError()))
}

func TestSourceNotifySessionErrorRejectsOverloadedWithOtherIssue(t *testing.T) {
	source := partition.NewSources((&mockTopicDescriber{}).Describe).Get("test/topic")

	assert.False(t, source.NotifySessionError(1, overloadedErrorWithIssue(42)))
}

func TestSourceNotifySessionErrorRejectsUnrelatedError(t *testing.T) {
	source := partition.NewSources((&mockTopicDescriber{}).Describe).Get("test/topic")

	assert.False(t, source.NotifySessionError(1, errors.New("session failed")))
}

func TestSourceNotifySessionErrorAcceptsPartitionInactiveIssue(t *testing.T) {
	source := partition.NewSources((&mockTopicDescriber{}).Describe).Get("test/topic")

	assert.True(t, source.NotifySessionError(1, partitionInactiveError()))
}

func TestSourcePartitionsWaitsForPublishedReplacement(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicWithActivePartitions(1),
		topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
			{PartitionID: 1, ChildPartitionIDs: []int64{2}},
		}},
		topicAfterReplacement(1, 2),
	)
	_, err := source.Partitions(t.Context())
	require.NoError(t, err)
	require.True(t, source.NotifySessionError(1, partitionInactiveError()))

	partitions, err := source.Partitions(t.Context())

	require.NoError(t, err)
	assert.False(t, partitions.ByPartitionID(1).IsActive())
	assert.True(t, partitions.ByPartitionID(2).IsActive())
}

func TestSourcePartitionsPublishesReplacementReportedInactiveDuringDescribe(t *testing.T) {
	unexpectedDescribe := errors.New("unexpected extra describe")
	var (
		source *partition.Source
		calls  atomic.Int64
	)
	source = partition.NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		switch calls.Add(1) {
		case 1:
			return topicWithActivePartitions(1), nil
		case 2:
			source.NotifySessionError(1, partitionInactiveError())

			return topicAfterReplacement(1, 2), nil
		default:
			return topictypes.TopicDescription{}, unexpectedDescribe
		}
	}).Get("test/topic")
	_, err := source.Partitions(t.Context())
	require.NoError(t, err)
	require.True(t, source.NotifySessionError(1, partitionInactiveError()))

	partitions, err := source.Partitions(t.Context())

	require.NoError(t, err)
	assert.Equal(t, int64(2), calls.Load())
	assert.True(t, partitions.ByPartitionID(2).IsActive())
}

func TestSourcePartitionsWaitsForReplacementThroughInactiveDescendants(t *testing.T) {
	source := newSourceWithDescriptions(
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
	_, err := source.Partitions(t.Context())
	require.NoError(t, err)
	require.True(t, source.NotifySessionError(1, partitionInactiveError()))

	partitions, err := source.Partitions(t.Context())

	require.NoError(t, err)
	assert.True(t, partitions.ByPartitionID(4).IsActive())
	assert.True(t, partitions.ByPartitionID(7).IsActive())
}

func TestSourcePartitionsReturnsContextErrorWhileWaitingForReplacement(t *testing.T) {
	var calls atomic.Int64
	refreshStarted := make(chan struct{})
	source := partition.NewSources(func(ctx context.Context, _ string) (topictypes.TopicDescription, error) {
		if calls.Add(1) == 1 {
			return topicWithActivePartitions(1), nil
		}
		close(refreshStarted)
		<-ctx.Done()

		return topictypes.TopicDescription{}, ctx.Err()
	}).Get("test/topic")
	_, err := source.Partitions(t.Context())
	require.NoError(t, err)
	require.True(t, source.NotifySessionError(1, partitionInactiveError()))
	refreshCtx, cancelRefresh := context.WithCancel(t.Context())
	result := startLoadingPartitions(refreshCtx, source)
	<-refreshStarted

	cancelRefresh()

	assert.ErrorIs(t, (<-result).err, context.Canceled)
}

func TestSourcePartitionsContinuesReplacementRefreshAfterDescribeError(t *testing.T) {
	describeErr := errors.New("describe topic failed")
	source := newSourceWithDescribeResults(
		topicDescribeResult{description: topicWithActivePartitions(1)},
		topicDescribeResult{err: describeErr},
		topicDescribeResult{description: topicAfterReplacement(1, 2)},
	)
	_, err := source.Partitions(t.Context())
	require.NoError(t, err)
	require.True(t, source.NotifySessionError(1, partitionInactiveError()))

	_, err = source.Partitions(t.Context())
	require.ErrorIs(t, err, describeErr)
	partitions, err := source.Partitions(t.Context())

	require.NoError(t, err)
	assert.True(t, partitions.ByPartitionID(2).IsActive())
}

func TestSourcePartitionsSharesReplacementRefresh(t *testing.T) {
	var calls atomic.Int64
	releaseRefresh := make(chan struct{})
	refreshStarted := make(chan struct{})
	source := partition.NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		if calls.Add(1) == 1 {
			return topicWithActivePartitions(1), nil
		}
		close(refreshStarted)
		<-releaseRefresh

		return topicAfterReplacement(1, 2), nil
	}).Get("test/topic")
	_, err := source.Partitions(t.Context())
	require.NoError(t, err)
	require.True(t, source.NotifySessionError(1, partitionInactiveError()))
	first := startLoadingPartitions(t.Context(), source)
	<-refreshStarted
	second := startWaitingForPartitions(t, source)

	close(releaseRefresh)

	require.NoError(t, (<-first).err)
	require.NoError(t, (<-second).err)
	assert.Equal(t, int64(2), calls.Load())
}

func TestSourceDoesNotUpdateRouter(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicAfterReplacement(1, 2),
	)
	initial, err := source.Partitions(t.Context())
	require.NoError(t, err)
	chooser := &recordingChooser{}
	_, err = partition.NewRouter(initial, chooser)
	require.NoError(t, err)
	require.True(t, source.NotifySessionError(1, partitionInactiveError()))

	_, err = source.Partitions(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []int64{1}, chooser.PartitionIDs())
}
