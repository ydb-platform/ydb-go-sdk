package partition

import (
	"context"
	"sync"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type topicDescribeResult struct {
	description topictypes.TopicDescription
	err         error
}

// scriptedTopicDescriber returns configured results in order and repeats the last one.
// It is safe to use when the test starts concurrent topology loads.
type scriptedTopicDescriber struct {
	mu      sync.Mutex
	results []topicDescribeResult
	next    int
}

type routeChangeResult struct {
	partitionID int64
	err         error
}

type partitionsResult struct {
	partitions *Partitions
	err        error
}

func newSourceWithDescriptions(descriptions ...topictypes.TopicDescription) *Source {
	results := make([]topicDescribeResult, len(descriptions))
	for i, description := range descriptions {
		results[i].description = description
	}

	return newSourceWithDescribeResults(results...)
}

func newSourceWithDescribeResults(results ...topicDescribeResult) *Source {
	describer := &scriptedTopicDescriber{results: results}

	return NewSources(describer.Describe).Get("test/topic")
}

func topicWithActivePartitions(partitionIDs ...int64) topictypes.TopicDescription {
	partitions := make([]topictypes.PartitionInfo, len(partitionIDs))
	for i, partitionID := range partitionIDs {
		partitions[i] = topictypes.PartitionInfo{PartitionID: partitionID, Active: true}
	}

	return topictypes.TopicDescription{Partitions: partitions}
}

func topicAfterReplacement(parentID, childID int64) topictypes.TopicDescription {
	return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
		{PartitionID: parentID, ChildPartitionIDs: []int64{childID}},
		{PartitionID: childID, Active: true, ParentPartitionIDs: []int64{parentID}},
	}}
}

func topicAfterMerge(parentIDs []int64, childID int64) topictypes.TopicDescription {
	partitions := make([]topictypes.PartitionInfo, 0, len(parentIDs)+1)
	for _, parentID := range parentIDs {
		partitions = append(partitions, topictypes.PartitionInfo{
			PartitionID:       parentID,
			ChildPartitionIDs: []int64{childID},
		})
	}
	partitions = append(partitions, topictypes.PartitionInfo{
		PartitionID:        childID,
		Active:             true,
		ParentPartitionIDs: parentIDs,
	})

	return topictypes.TopicDescription{Partitions: partitions}
}

func startNewRouter(ctx context.Context, source *Source, chooser Chooser) <-chan error {
	result := make(chan error, 1)
	go func() {
		_, err := source.NewRouter(ctx, chooser)
		result <- err
	}()

	return result
}

func startWaitingForRouteChange(router *Router) <-chan routeChangeResult {
	result := make(chan routeChangeResult, 1)
	go func() {
		partitionID, err := router.WaitForRouteChange()
		result <- routeChangeResult{partitionID: partitionID, err: err}
	}()

	return result
}

func startLoadingPartitions(ctx context.Context, source *Source) <-chan partitionsResult {
	result := make(chan partitionsResult, 1)
	go func() {
		partitions, err := source.Partitions(ctx)
		result <- partitionsResult{partitions: partitions, err: err}
	}()

	return result
}

func startWaitingForPartitions(t *testing.T, source *Source) <-chan partitionsResult {
	t.Helper()

	waiting := make(chan struct{})
	waitCtx := &doneObservedContext{Context: t.Context(), observed: waiting}
	result := startLoadingPartitions(waitCtx, source)
	<-waiting

	return result
}

func (d *scriptedTopicDescriber) Describe(
	ctx context.Context,
	_ string,
) (topictypes.TopicDescription, error) {
	if err := ctx.Err(); err != nil {
		return topictypes.TopicDescription{}, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	result := d.results[d.next]
	if d.next < len(d.results)-1 {
		d.next++
	}

	return result.description, result.err
}

func overloadedError() error {
	return xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))
}

func partitionInactiveError() error {
	return overloadedErrorWithIssue(xerrors.IssueCodeTopicPartitionInactive)
}

func overloadedErrorWithIssue(issueCode uint32) error {
	return xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED),
		xerrors.WithIssues([]*Ydb_Issue.IssueMessage{{IssueCode: issueCode}}),
	)
}
