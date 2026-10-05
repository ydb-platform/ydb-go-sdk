package partition_test

import (
	"context"
	"sync"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/partition"
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

type partitionsResult struct {
	partitions *partition.Partitions
	err        error
}

func newTopicTopologyWithDescriptions(descriptions ...topictypes.TopicDescription) *partition.TopicTopology {
	results := make([]topicDescribeResult, len(descriptions))
	for i, description := range descriptions {
		results[i].description = description
	}

	return newTopicTopologyWithDescribeResults(results...)
}

func newTopicTopologyWithDescribeResults(results ...topicDescribeResult) *partition.TopicTopology {
	describer := &scriptedTopicDescriber{results: results}

	return partition.NewTopologyRegistry(describer.Describe).Get("test/topic")
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

func startLoadingPartitions(ctx context.Context, topology *partition.TopicTopology) <-chan partitionsResult {
	result := make(chan partitionsResult, 1)
	go func() {
		partitions, err := topology.Partitions(ctx)
		result <- partitionsResult{partitions: partitions, err: err}
	}()

	return result
}

func startWaitingForPartitions(t *testing.T, topology *partition.TopicTopology) <-chan partitionsResult {
	t.Helper()

	waiting := make(chan struct{})
	waitCtx := newDoneObservedContext(t.Context(), waiting)
	result := startLoadingPartitions(waitCtx, topology)
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
