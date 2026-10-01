package transactionalwriterbenchmark

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
	)

	if specs[0].Key != "worker-3-message-7" {
		t.Fatalf("Key = %q, want %q", specs[0].Key, "worker-3-message-7")
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
	)

	if specs[0].Key != "" {
		t.Fatalf("single writer key = %q, want unset", specs[0].Key)
	}
}

func TestQuoteYQLPath(t *testing.T) {
	t.Parallel()

	if got := quoteYQLPath("dir/table"); got != "`dir/table`" {
		t.Fatalf("quoteYQLPath() = %q, want %q", got, "`dir/table`")
	}
}
