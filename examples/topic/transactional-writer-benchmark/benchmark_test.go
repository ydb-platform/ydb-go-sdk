package main

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestFixedTopicUsesPausedAutoPartitioningToExposeBounds(t *testing.T) {
	t.Parallel()

	settings := topicAutoPartitioningSettings(config{Routing: routingModeBoundedKey})
	if settings.AutoPartitioningStrategy != topictypes.AutoPartitioningStrategyPaused {
		t.Fatalf(
			"AutoPartitioningStrategy = %v, want paused",
			settings.AutoPartitioningStrategy,
		)
	}
}

func TestFixedHashTopicDisablesAutoPartitioningToAvoidBounds(t *testing.T) {
	t.Parallel()

	settings := topicAutoPartitioningSettings(config{Routing: routingModeKey})
	if settings.AutoPartitioningStrategy != topictypes.AutoPartitioningStrategyDisabled {
		t.Fatalf(
			"AutoPartitioningStrategy = %v, want disabled",
			settings.AutoPartitioningStrategy,
		)
	}
}

func TestMakeMessageSpecsRoutesAcrossActivePartitions(t *testing.T) {
	t.Parallel()

	specs := makeMessageSpecs(
		config{
			Mode:          writerModeMany,
			Routing:       routingModePartitionID,
			AutoSeqNo:     false,
			MessagesPerTx: 3,
		},
		0,
		2,
		[]int64{2, 4},
	)

	wantPartitions := []int64{4, 2, 4}
	wantSeqNos := []int64{4, 5, 6}
	for i := range specs {
		if specs[i].PartitionID != wantPartitions[i] {
			t.Fatalf("specs[%d].PartitionID = %d, want %d", i, specs[i].PartitionID, wantPartitions[i])
		}
		if specs[i].SeqNo != wantSeqNos[i] {
			t.Fatalf("specs[%d].SeqNo = %d, want %d", i, specs[i].SeqNo, wantSeqNos[i])
		}
	}
}

func TestMakeMessageSpecsUsesKeysForKeyRouting(t *testing.T) {
	t.Parallel()

	specs := makeMessageSpecs(
		config{
			Mode:          writerModeMany,
			Routing:       routingModeKey,
			AutoSeqNo:     true,
			MessagesPerTx: 1,
		},
		3,
		7,
		[]int64{0, 1, 2, 3},
	)

	if specs[0].Key != "worker-3-message-7" {
		t.Fatalf("Key = %q, want %q", specs[0].Key, "worker-3-message-7")
	}
	if specs[0].PartitionID != 0 {
		t.Fatalf("PartitionID = %d, want zero for key routing", specs[0].PartitionID)
	}
	if specs[0].SeqNo != 0 {
		t.Fatalf("SeqNo = %d, want zero with automatic sequence numbers", specs[0].SeqNo)
	}
}

func TestMakeMessageSpecsUsesKeysForBoundedKeyRouting(t *testing.T) {
	t.Parallel()

	specs := makeMessageSpecs(
		config{
			Mode:          writerModeMany,
			Routing:       routingModeBoundedKey,
			AutoSeqNo:     true,
			MessagesPerTx: 1,
		},
		2,
		5,
		[]int64{0},
	)

	if specs[0].Key != "worker-2-message-5" {
		t.Fatalf("Key = %q, want %q", specs[0].Key, "worker-2-message-5")
	}
}

func TestMakeMessageSpecsLeavesSingleWriterRoutingUnset(t *testing.T) {
	t.Parallel()

	specs := makeMessageSpecs(
		config{
			Mode:          writerModeSingle,
			Routing:       routingModeKey,
			AutoSeqNo:     true,
			MessagesPerTx: 1,
		},
		0,
		1,
		[]int64{0},
	)

	if specs[0].Key != "" || specs[0].PartitionID != 0 {
		t.Fatalf("single writer routing = {%q, %d}, want unset", specs[0].Key, specs[0].PartitionID)
	}
}

func TestQuoteYQLPath(t *testing.T) {
	t.Parallel()

	if got := quoteYQLPath("dir/table"); got != "`dir/table`" {
		t.Fatalf("quoteYQLPath() = %q, want %q", got, "`dir/table`")
	}
}
