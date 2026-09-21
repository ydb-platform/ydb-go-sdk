package partition

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestSourceNewRouterReturnsRouter(t *testing.T) {
	describer := &mockTopicDescriber{}
	source := NewSources(describer.Describe).Get("test/topic")

	router, _ := source.NewRouter(t.Context(), nil)
	assert.NotNil(t, router)
}

func TestSourceNewRouterReturnsDescribeError(t *testing.T) {
	describeErr := errors.New("describe topic failed")
	source := NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		return topictypes.TopicDescription{}, describeErr
	}).Get("test/topic")

	_, err := source.NewRouter(t.Context(), nil)
	assert.ErrorIs(t, err, describeErr)
}

func TestSourceNewRouterReturnsContextErrorForCachedPartitions(t *testing.T) {
	source := NewSources((&mockTopicDescriber{}).Describe).Get("test/topic")
	_, _ = source.Partitions(t.Context())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := source.NewRouter(ctx, nil)

	assert.ErrorIs(t, err, context.Canceled)
}

func TestSourceNewRouterDescribesRequestedTopic(t *testing.T) {
	topicName := "test/topic"
	describer := &mockTopicDescriber{}
	source := NewSources(describer.Describe).Get(topicName)

	_, _ = source.NewRouter(t.Context(), nil)
	assert.Equal(t, topicName, describer.Calls()[0].Path)
}

func TestSourceNewRouterAddsActivePartitionToChooser(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 42, Active: true}},
	}}
	chooser := &recordingChooser{}

	_, _ = NewSources(describer.Describe).Get("test/topic").NewRouter(t.Context(), chooser)

	assert.Equal(t, []int64{42}, chooser.PartitionIDs())
}

func TestSourceNewRouterDoesNotAddInactivePartitionToChooser(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{
			{PartitionID: 1, Active: true},
			{PartitionID: 2},
		},
	}}
	chooser := &recordingChooser{}

	_, _ = NewSources(describer.Describe).Get("test/topic").NewRouter(t.Context(), chooser)

	assert.Equal(t, []int64{1}, chooser.PartitionIDs())
}

func TestSourceNewRouterReturnsChooserInitializationError(t *testing.T) {
	chooserErr := errors.New("initialize chooser")
	source := newSourceWithDescriptions(topicWithActivePartitions(1))

	_, err := source.NewRouter(t.Context(), &errorChooser{addErr: chooserErr})

	assert.ErrorIs(t, err, chooserErr)
}

func TestSourceNewRouterReturnsConcurrentChooserUpdateError(t *testing.T) {
	updateErr := errors.New("update chooser")
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicWithActivePartitions(2),
	)
	chooser := &secondAddErrorChooser{
		started: make(chan struct{}),
		release: make(chan struct{}),
		err:     updateErr,
	}
	result := startNewRouter(t.Context(), source, chooser)
	<-chooser.started
	source.Invalidate()
	_, _ = source.Partitions(t.Context())
	close(chooser.release)

	assert.ErrorIs(t, <-result, updateErr)
}

func TestSourceUpdatesRouterChooserAfterReplacement(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicAfterReplacement(1, 2),
	)
	chooser := &recordingChooser{}
	_, _ = source.NewRouter(t.Context(), chooser)
	waitForReplacement := source.NotifySessionError(t.Context(), 1, partitionInactiveError())

	require.NoError(t, waitForReplacement(t.Context()))

	assert.Equal(t, []int64{2}, chooser.PartitionIDs())
}

func TestSourceNewRouterDoesNotMissConcurrentReplacement(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicAfterReplacement(1, 2),
	)
	_, _ = source.Partitions(t.Context())
	chooser := &blockingChooser{started: make(chan struct{}), release: make(chan struct{})}
	routerResult := startNewRouter(t.Context(), source, chooser)
	<-chooser.started
	waitForReplacement := source.NotifySessionError(t.Context(), 1, partitionInactiveError())
	require.NoError(t, waitForReplacement(t.Context()))
	close(chooser.release)
	require.NoError(t, <-routerResult)

	assert.Equal(t, []int64{2}, chooser.PartitionIDs())
}

func TestSourceNewRouterDoesNotUpdateChooserWhenLifetimeEndsDuringInitialization(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicWithActivePartitions(2),
	)
	_, _ = source.Partitions(t.Context())
	chooser := &blockingChooser{started: make(chan struct{}), release: make(chan struct{})}
	routerCtx, cancelRouter := context.WithCancel(t.Context())
	result := startNewRouter(routerCtx, source, chooser)
	<-chooser.started
	source.Invalidate()
	_, _ = source.Partitions(t.Context())
	cancelRouter()
	close(chooser.release)
	require.NoError(t, <-result)

	assert.Equal(t, []int64{1}, chooser.PartitionIDs())
}

func TestSourcePartitionsReturnsPartitions(t *testing.T) {
	describer := &mockTopicDescriber{}
	source := NewSources(describer.Describe).Get("test/topic")

	partitions, _ := source.Partitions(t.Context())
	assert.NotNil(t, partitions)
}

func TestSourcePartitionsReturnsDescribeError(t *testing.T) {
	describeErr := errors.New("describe topic failed")
	source := NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		return topictypes.TopicDescription{}, describeErr
	}).Get("test/topic")

	_, err := source.Partitions(t.Context())
	assert.ErrorIs(t, err, describeErr)
}

func TestSourcePartitionsReturnsDescribedPartitionIDs(t *testing.T) {
	describer := &mockTopicDescriber{description: topictypes.TopicDescription{
		Partitions: []topictypes.PartitionInfo{{PartitionID: 42, Active: true}},
	}}
	source := NewSources(describer.Describe).Get("test/topic")

	partitions, _ := source.Partitions(t.Context())
	assert.Equal(t, []int64{42}, partitions.All().IDs())
}

func TestSourcePartitionsDescribesTopicOnce(t *testing.T) {
	describer := &mockTopicDescriber{}
	source := NewSources(describer.Describe).Get("test/topic")

	_, _ = source.Partitions(t.Context())
	_, _ = source.Partitions(t.Context())

	assert.Len(t, describer.Calls(), 1)
}

func TestSourcePartitionsDescribesTopicOnceConcurrently(t *testing.T) {
	ctx := t.Context()
	describer := &mockTopicDescriber{}
	source := NewSources(describer.Describe).Get("test/topic")
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
	source := NewSources(func(ctx context.Context, _ string) (topictypes.TopicDescription, error) {
		if calls.Add(1) == 1 {
			close(describeStarted)
			<-ctx.Done()

			return topictypes.TopicDescription{}, ctx.Err()
		}

		return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{{PartitionID: 42}}}, nil
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
	source := NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
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
	source := NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		partitionID := calls.Add(1)
		if partitionID == 1 {
			close(describeStarted)
			<-releaseDescribe
		}

		return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
			{PartitionID: partitionID},
		}}, nil
	}).Get("test/topic")
	result := startLoadingPartitions(t.Context(), source)
	<-describeStarted

	source.Invalidate()
	close(releaseDescribe)

	assert.Equal(t, []int64{2}, (<-result).partitions.All().IDs())
}

func TestSourcePartitionsWaitsForRouterUpdateBeforePublishing(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicAfterReplacement(1, 2),
	)
	chooser := &blockingUpdateChooser{started: make(chan struct{}), release: make(chan struct{})}
	_, _ = source.NewRouter(t.Context(), chooser)
	source.Invalidate()
	loadResult := startLoadingPartitions(t.Context(), source)
	<-chooser.started
	waitCtx, cancelWait := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancelWait()

	_, err := source.Partitions(waitCtx)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	close(chooser.release)
	require.NoError(t, (<-loadResult).err)
}

func TestSourceNewRouterAndPartitionsDescribeTopicOnce(t *testing.T) {
	describer := &mockTopicDescriber{}
	source := NewSources(describer.Describe).Get("test/topic")

	_, _ = source.NewRouter(t.Context(), nil)
	_, _ = source.Partitions(t.Context())

	assert.Len(t, describer.Calls(), 1)
}

func TestSourcePartitionsReloadsAfterInvalidate(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicWithActivePartitions(2),
	)
	_, _ = source.Partitions(t.Context())

	source.Invalidate()
	partitions, _ := source.Partitions(t.Context())

	assert.Equal(t, []int64{2}, partitions.All().IDs())
}

func TestSourceNotifySessionErrorRejectsOverloadedWithoutPartitionInactiveIssue(t *testing.T) {
	source := NewSources((&mockTopicDescriber{}).Describe).Get("test/topic")

	assert.Nil(t, source.NotifySessionError(t.Context(), 1, overloadedError()))
}

func TestSourceNotifySessionErrorRejectsOverloadedWithOtherIssue(t *testing.T) {
	source := NewSources((&mockTopicDescriber{}).Describe).Get("test/topic")

	assert.Nil(t, source.NotifySessionError(t.Context(), 1, overloadedErrorWithIssue(42)))
}

func TestSourceNotifySessionErrorHandlesPartitionInactiveIssue(t *testing.T) {
	source := NewSources((&mockTopicDescriber{}).Describe).Get("test/topic")

	assert.NotNil(t, source.NotifySessionError(t.Context(), 1, partitionInactiveError()))
}

func TestSourceNotifySessionErrorHandlesAlreadyPublishedReplacement(t *testing.T) {
	source := newSourceWithDescriptions(topicAfterReplacement(1, 2))
	_, _ = source.Partitions(t.Context())

	waitForReplacement := source.NotifySessionError(t.Context(), 1, partitionInactiveError())

	require.NotNil(t, waitForReplacement)
	assert.NoError(t, waitForReplacement(t.Context()))
}

func TestSourceNotifySessionErrorRejectsUnrelatedError(t *testing.T) {
	source := NewSources((&mockTopicDescriber{}).Describe).Get("test/topic")

	assert.Nil(t, source.NotifySessionError(t.Context(), 1, errors.New("session failed")))
}

func TestSourceNotifySessionErrorRejectsCanceledContext(t *testing.T) {
	source := NewSources((&mockTopicDescriber{}).Describe).Get("test/topic")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	overloaded := partitionInactiveError()

	assert.Nil(t, source.NotifySessionError(ctx, 1, overloaded))
}

func TestSourceNotifySessionErrorWaiterReturnsCallerContextError(t *testing.T) {
	var calls atomic.Int64
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	source := NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		if calls.Add(1) == 1 {
			return topicWithActivePartitions(1), nil
		}
		close(refreshStarted)
		<-releaseRefresh

		return topicAfterReplacement(1, 2), nil
	}).Get("test/topic")
	_, _ = source.Partitions(t.Context())
	overloaded := partitionInactiveError()
	waitForReplacement := source.NotifySessionError(t.Context(), 1, overloaded)
	<-refreshStarted
	waitCtx, cancelWait := context.WithCancel(t.Context())
	cancelWait()

	err := waitForReplacement(waitCtx)

	assert.ErrorIs(t, err, context.Canceled)
	close(releaseRefresh)
	require.NoError(t, waitForReplacement(t.Context()))
}

func TestSourceNotifySessionErrorWaiterPublishesReplacement(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicAfterReplacement(1, 2),
	)
	_, _ = source.Partitions(t.Context())
	overloaded := partitionInactiveError()
	waitForReplacement := source.NotifySessionError(t.Context(), 1, overloaded)

	require.NoError(t, waitForReplacement(t.Context()))
	partitions, _ := source.Partitions(t.Context())

	assert.False(t, partitions.ByPartitionID(1).IsActive())
}

func TestSourceNotifySessionErrorWaiterRetriesStaleDescription(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicWithActivePartitions(1),
		topicAfterReplacement(1, 2),
	)
	_, _ = source.Partitions(t.Context())
	overloaded := partitionInactiveError()
	waitForReplacement := source.NotifySessionError(t.Context(), 1, overloaded)

	require.NoError(t, waitForReplacement(t.Context()))
	partitions, _ := source.Partitions(t.Context())

	assert.False(t, partitions.ByPartitionID(1).IsActive())
}

func TestSourceNotifySessionErrorWaiterRetriesIncompleteReplacementDescription(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
			{PartitionID: 1, ChildPartitionIDs: []int64{2}},
		}},
		topicAfterReplacement(1, 2),
	)
	_, _ = source.Partitions(t.Context())
	waitForReplacement := source.NotifySessionError(t.Context(), 1, partitionInactiveError())

	require.NoError(t, waitForReplacement(t.Context()))
	partitions, _ := source.Partitions(t.Context())

	assert.True(t, partitions.ByPartitionID(2).IsActive())
}

func TestSourceReplacementRefreshSurvivesOneReporterCancellation(t *testing.T) {
	var calls atomic.Int64
	refreshStarted := make(chan struct{})
	checkCancellation := make(chan struct{})
	releaseRefresh := make(chan struct{})
	source := NewSources(func(ctx context.Context, _ string) (topictypes.TopicDescription, error) {
		if calls.Add(1) == 1 {
			return topicWithActivePartitions(1), nil
		}
		close(refreshStarted)
		<-checkCancellation
		if err := ctx.Err(); err != nil {
			return topictypes.TopicDescription{}, err
		}
		<-releaseRefresh

		return topicAfterReplacement(1, 2), nil
	}).Get("test/topic")
	_, _ = source.Partitions(t.Context())
	overloaded := partitionInactiveError()
	firstReporter, cancelFirstReporter := context.WithCancel(t.Context())
	_ = source.NotifySessionError(firstReporter, 1, overloaded)
	<-refreshStarted
	waitForReplacement := source.NotifySessionError(t.Context(), 1, overloaded)

	cancelFirstReporter()
	close(checkCancellation)
	close(releaseRefresh)

	require.NoError(t, waitForReplacement(t.Context()))
}

func TestSourceCanceledReplacementRefreshDoesNotFailOtherRouter(t *testing.T) {
	var calls atomic.Int64
	refreshStarted := make(chan struct{})
	source := NewSources(func(ctx context.Context, _ string) (topictypes.TopicDescription, error) {
		if calls.Add(1) == 1 {
			return topicWithActivePartitions(1), nil
		}
		close(refreshStarted)
		<-ctx.Done()

		return topictypes.TopicDescription{}, ctx.Err()
	}).Get("test/topic")
	router, _ := source.NewRouter(t.Context(), &fixedChooser{partitionID: 1})
	reporterCtx, cancelReporter := context.WithCancel(t.Context())
	overloaded := partitionInactiveError()
	waitForReplacement := source.NotifySessionError(reporterCtx, 1, overloaded)
	<-refreshStarted
	cancelReporter()
	require.ErrorIs(t, waitForReplacement(t.Context()), context.Canceled)

	_, err := router.ChoosePartition(topicwriterinternal.PublicMessage{})

	assert.NoError(t, err)
}

func TestSourceRetriesCompletedReplacementRefreshForNewRouter(t *testing.T) {
	describeErr := errors.New("describe topic failed")
	source := newSourceWithDescribeResults(
		topicDescribeResult{description: topicWithActivePartitions(1)},
		topicDescribeResult{err: describeErr},
		topicDescribeResult{description: topicWithActivePartitions(1)},
		topicDescribeResult{description: topicAfterReplacement(1, 2)},
	)
	overloaded := partitionInactiveError()
	_, _ = source.NewRouter(t.Context(), &recordingChooser{})
	waitForReplacement := source.NotifySessionError(t.Context(), 1, overloaded)
	require.ErrorIs(t, waitForReplacement(t.Context()), describeErr)
	_, err := source.NewRouter(t.Context(), &recordingChooser{})
	require.NoError(t, err)
	waitForReplacement = source.NotifySessionError(t.Context(), 1, overloaded)

	err = waitForReplacement(t.Context())

	assert.NoError(t, err)
}
