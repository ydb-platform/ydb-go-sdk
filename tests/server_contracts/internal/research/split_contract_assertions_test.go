package research_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
)

func TestSplitDiscoveryAllowsDelayedOrImmediateChildren(t *testing.T) {
	rejected := time.Unix(100, 0)
	old := &Ydb_Topic.DescribeTopicResult{Partitions: []*Ydb_Topic.DescribeTopicResult_PartitionInfo{
		{PartitionId: 0, Active: true},
	}}
	ready := &Ydb_Topic.DescribeTopicResult{Partitions: []*Ydb_Topic.DescribeTopicResult_PartitionInfo{
		{PartitionId: 0, ChildPartitionIds: []int64{2, 1}},
		{PartitionId: 1, Active: true, ParentPartitionIds: []int64{0}},
		{PartitionId: 2, Active: true, ParentPartitionIds: []int64{0}},
	}}
	stale := topicDescriptionSample{started: rejected.Add(time.Millisecond), result: old}
	fresh := topicDescriptionSample{started: rejected.Add(2 * time.Millisecond), result: ready}
	for _, test := range []struct {
		name    string
		samples []topicDescriptionSample
		want    bool
	}{
		{name: "delayed children", samples: []topicDescriptionSample{stale, fresh}, want: true},
		{name: "immediate children", samples: []topicDescriptionSample{fresh}, want: true},
		{name: "children never appeared", samples: []topicDescriptionSample{stale}},
		{name: "no observation"},
		{name: "request predates rejection", samples: []topicDescriptionSample{
			{started: rejected.Add(-time.Millisecond), finished: rejected.Add(time.Millisecond), result: ready},
		}},
		{name: "wrong children", samples: []topicDescriptionSample{
			{started: fresh.started, result: &Ydb_Topic.DescribeTopicResult{Partitions: ready.GetPartitions()[:2]}},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation := &splitObservation{writeFailed: rejected, samples: test.samples}
			if got := splitChildrenDiscovered(observation, 0, []int64{1, 2}); got != test.want {
				t.Fatalf("children discovered: got %t, want %t", got, test.want)
			}
		})
	}
}

func TestSplitStreamTerminationRequiresServerStatusAndEOF(t *testing.T) {
	for _, test := range []struct {
		name   string
		status Ydb.StatusIds_StatusCode
		end    error
		want   bool
	}{
		{name: "overloaded and EOF", status: Ydb.StatusIds_OVERLOADED, end: io.EOF, want: true},
		{name: "wrong server status", status: Ydb.StatusIds_BAD_REQUEST, end: io.EOF},
		{name: "success and EOF", status: Ydb.StatusIds_SUCCESS, end: io.EOF},
		{name: "transport error", status: Ydb.StatusIds_OVERLOADED, end: io.ErrUnexpectedEOF},
	} {
		t.Run(test.name, func(t *testing.T) {
			research := &streamWriteResearch{}
			_, wire := startControlledSession(t, research, "Parent")
			ctx := context.WithValue(t.Context(), worldContextKey{}, &researchWorld{research: research})
			wire.received <- streamWriteReceive{message: &Ydb_Topic.StreamWriteMessage_FromServer{
				Status: Ydb.StatusIds_SUCCESS,
				ServerMessage: &Ydb_Topic.StreamWriteMessage_FromServer_InitResponse{
					InitResponse: &Ydb_Topic.StreamWriteMessage_InitResponse{},
				},
			}}
			wire.received <- streamWriteReceive{message: &Ydb_Topic.StreamWriteMessage_FromServer{Status: test.status}}
			wire.received <- streamWriteReceive{err: test.end}
			if err := contractStreamTerminated(ctx, "Parent", "OVERLOADED"); (err == nil) != test.want {
				t.Fatalf("expected valid=%t, got %v", test.want, err)
			}
		})
	}
}
