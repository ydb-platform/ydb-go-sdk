package partition

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouterWaitForRouteChangeReturnsLifetimeContextError(t *testing.T) {
	routerCtx, cancelRouter := context.WithCancel(t.Context())
	router, _ := NewSources((&mockTopicDescriber{}).Describe).Get("test/topic").NewRouter(routerCtx, nil)
	cancelRouter()

	_, err := router.WaitForRouteChange()

	assert.ErrorIs(t, err, context.Canceled)
}

func TestRouterWaitForRouteChangeReturnsReplacement(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicAfterReplacement(1, 2),
	)
	router, _ := source.NewRouter(t.Context(), &recordingChooser{})
	result := startWaitingForRouteChange(router)
	waitForReplacement := source.NotifySessionError(t.Context(), 1, partitionInactiveError())
	require.NoError(t, waitForReplacement(t.Context()))

	update := <-result

	require.NoError(t, update.err)
	assert.Equal(t, int64(1), update.partitionID)
}

func TestSourcePublishesReplacementToEveryRouter(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicAfterReplacement(1, 2),
	)
	routers := make([]*Router, 2)
	for i := range routers {
		routers[i], _ = source.NewRouter(t.Context(), nil)
	}
	waitForReplacement := source.NotifySessionError(t.Context(), 1, partitionInactiveError())
	require.NoError(t, waitForReplacement(t.Context()))

	for _, router := range routers {
		partitionID, err := router.WaitForRouteChange()
		require.NoError(t, err)
		assert.Equal(t, int64(1), partitionID)
	}
}

func TestRouterWaitForRouteChangeIgnoresTopologyChangeWithoutReplacement(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicWithActivePartitions(1, 2),
		topicAfterReplacement(2, 3),
	)
	router, _ := source.NewRouter(t.Context(), &recordingChooser{})
	source.Invalidate()
	_, _ = source.Partitions(t.Context())
	result := startWaitingForRouteChange(router)
	waitForReplacement := source.NotifySessionError(t.Context(), 2, partitionInactiveError())
	require.NoError(t, waitForReplacement(t.Context()))

	update := <-result

	require.NoError(t, update.err)
	assert.Equal(t, int64(2), update.partitionID)
}

func TestRouterWaitForRouteChangeReturnsEveryReplacedParentAfterMerge(t *testing.T) {
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1, 2),
		topicAfterMerge([]int64{1, 2}, 3),
	)
	router, _ := source.NewRouter(t.Context(), &recordingChooser{})
	waitForReplacement := source.NotifySessionError(t.Context(), 1, partitionInactiveError())
	require.NoError(t, waitForReplacement(t.Context()))

	replaced := make([]int64, 0, 2)
	for range 2 {
		partitionID, err := router.WaitForRouteChange()
		require.NoError(t, err)
		replaced = append(replaced, partitionID)
	}
	assert.ElementsMatch(t, []int64{1, 2}, replaced)
}

func TestRouterWaitForRouteChangeReturnsRefreshError(t *testing.T) {
	describeErr := errors.New("describe topic failed")
	source := newSourceWithDescribeResults(
		topicDescribeResult{description: topicWithActivePartitions(1)},
		topicDescribeResult{err: describeErr},
	)
	router, _ := source.NewRouter(t.Context(), &recordingChooser{})
	waitForReplacement := source.NotifySessionError(t.Context(), 1, partitionInactiveError())
	require.ErrorIs(t, waitForReplacement(t.Context()), describeErr)

	_, err := router.WaitForRouteChange()

	assert.ErrorIs(t, err, describeErr)
}

func TestRouterWaitForRouteChangeReturnsChooserUpdateErrorOnlyToAffectedRouter(t *testing.T) {
	updateErr := errors.New("update chooser")
	source := newSourceWithDescriptions(
		topicWithActivePartitions(1),
		topicAfterReplacement(1, 2),
	)
	failedRouter, _ := source.NewRouter(t.Context(), &updateErrorChooser{err: updateErr})
	updatedChooser := &recordingChooser{}
	_, _ = source.NewRouter(t.Context(), updatedChooser)

	waitForReplacement := source.NotifySessionError(t.Context(), 1, partitionInactiveError())
	require.NoError(t, waitForReplacement(t.Context()))

	_, err := failedRouter.WaitForRouteChange()
	assert.ErrorIs(t, err, updateErr)
	assert.Equal(t, []int64{2}, updatedChooser.PartitionIDs())
}
