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

func TestManyWriterMessageKey(t *testing.T) {
	t.Parallel()

	runner := transactionRunner{messageKeyPrefix: "worker-3-message-"}
	if got := runner.messageKey(7); got != "worker-3-message-7" {
		t.Fatalf("messageKey() = %q, want %q", got, "worker-3-message-7")
	}
}

func TestQuoteYQLPath(t *testing.T) {
	t.Parallel()

	if got := quoteYQLPath("dir/table"); got != "`dir/table`" {
		t.Fatalf("quoteYQLPath() = %q, want %q", got, "`dir/table`")
	}
}
