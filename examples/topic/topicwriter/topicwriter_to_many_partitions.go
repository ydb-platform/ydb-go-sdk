// This file illustrates how to configure a writer that sends data to multiple topic partitions behind a single API.
//
// Behaviour that matters for applications:
//
// Ordering: The server preserves order of messages for the same partitioning key inside the partition they map to—paired
// with sequential sequence numbers under a stable producer identity, retries do not reorder visible delivery.
//
// Exactly-once: YDB persists each message under a logical producer identifier and sequence number pair; duplicates with
// the same pair are suppressed. Prefix all writers with WithProducerIDPrefix (or configure producer IDs explicitly) and
// keep sequence numbers strictly increasing per producer partition session (automatic assignment is easiest).
//
// Auto-split partitions: Topics can scale out with server-side partitioning. Bound-style key routing reacts to splits by
// taking updated partition boundaries from DescribeTopic; keyed messages keep mapping to disjoint ranges so new
// partitions are picked up as they appear without changing producer code.
//
// Experimental: Writing to many partitions is experimental in the SDK; see VERSIONING.md.
//
// The load-focused binary under ../topicmultiwriter complements this snippet; integration scenarios live in topic tests.
//
// Production applications only expose the topicoptions knobs; internal testing hooks (writers factory swaps, custom server
// chooser wiring without going through Bound/Kafka wrappers) intentionally stay SDK-private.
package topicwriter

import (
	"bytes"
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

// StartWriterAcrossPartitionsBoundedKey opens a writer with key-based Bound routing (recommended when you rely on
// automatic partition growth). Inline comments map to every applicable public knob from topicoptions.MultiWriterOption.
//
// Routing strategies are mutually exclusive: see StartWriterAcrossPartitionsExplicitID for PartitionID routing, and
// KafkaCompatibleKeyRoutingOptions for Kafka-hash routing.
func StartWriterAcrossPartitionsBoundedKey(ctx context.Context, db *ydb.Driver, topicPath string) (*topicwriter.Writer, error) {
	return db.Topic().StartWriter(topicPath,
		topicoptions.WithWriteToManyPartitions(
			// WithProducerIDPrefix defines the prefix for internally generated producer IDs; it participates in persistence
			// keys and contributes to duplicate suppression together with sequence numbers chosen per outbound stream.
			topicoptions.WithProducerIDPrefix("docs-example-bounded"),

			// WithWriterIdleTimeout shuts down dormant per-partition sessions after silence to release resources when traffic
			// is intermittent; short values increase churn, larger values reuse streams longer.
			topicoptions.WithWriterIdleTimeout(30*time.Minute),

			// BoundPartitionChooser places messages by comparing a hashed partitioning key against ordered partition bounds.
			// It is compatible with Topic auto partitioning that grows partition count over time—the client refreshes bounds.
			topicoptions.WithWriterPartitionByKey(topicoptions.BoundPartitionChooser(
				// Optional: customise how raw Message.Key becomes the string matched against boundaries. The default hashes
				// like legacy C++/Go producers; overriding is only needed when you must align routing with upstream systems.
				topicoptions.WithBoundPartitionChooserPartitioningKeyHasher(func(key string) string { return key }),
			)),
		),
		// WithWriterWaitServerAck enforces acknowledgement before Write returns—a common choice for transactional safety.
		topicoptions.WithWriterWaitServerAck(true),
		// WithWriterSetAutoSeqNo keeps sequence numbers strictly increasing per producer/stream without assigning them manually.
		topicoptions.WithWriterSetAutoSeqNo(true),
	)
}

// KafkaCompatibleKeyRoutingOptions swaps Bound routing for Kafka-style Murmur hashing over Message.Key.
//
// Prefer when migrating Kafka producers and relying on hash % partitionCount instead of server numeric bounds—the current
// partition list still refreshes during auto-split, but hashing stays compatible with Kafka partitioning math.
func KafkaCompatibleKeyRoutingOptions(producerPrefix string) []topicoptions.MultiWriterOption {
	return []topicoptions.MultiWriterOption{
		topicoptions.WithProducerIDPrefix(producerPrefix),
		topicoptions.WithWriterIdleTimeout(30 * time.Minute),
		topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
	}
}

// StartWriterAcrossPartitionsExplicitID opens a writer that routes purely by Message.PartitionID.
//
// Use WithWriterPartitionByPartitionID; do not combine with keyed routing helpers.
func StartWriterAcrossPartitionsExplicitID(ctx context.Context, db *ydb.Driver, topicPath string) (*topicwriter.Writer, error) {
	return db.Topic().StartWriter(topicPath,
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithProducerIDPrefix("docs-example-partition-id"),

			topicoptions.WithWriterIdleTimeout(45*time.Minute),

			// Routes using Message.PartitionID; keyed metadata is irrelevant.
			topicoptions.WithWriterPartitionByPartitionID(),
		),
		topicoptions.WithWriterWaitServerAck(true),
		topicoptions.WithWriterSetAutoSeqNo(true),
	)
}

// StartTransactionalWriterAcrossPartitions mirrors StartWriterAcrossPartitionsBoundedKey inside query.DoTx workloads.
//
// Typical production DoTx calls also pass query.WithIdempotent when your handler must survive retries.
func StartTransactionalWriterAcrossPartitions(
	ctx context.Context,
	db *ydb.Driver,
	tx query.TxActor,
	topicPath string,
) (*topicwriter.TxWriter, error) {
	w, err := db.Topic().StartTransactionalWriter(tx, topicPath,
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithProducerIDPrefix("docs-example-tx"),

			topicoptions.WithWriterIdleTimeout(30*time.Minute),

			topicoptions.WithWriterPartitionByKey(topicoptions.BoundPartitionChooser(
				topicoptions.WithBoundPartitionChooserPartitioningKeyHasher(func(key string) string { return key }),
			)),
		),
		topicoptions.WithWriterWaitServerAck(true),
		topicoptions.WithWriterSetAutoSeqNo(true),
	)
	if err != nil {
		return nil, err
	}
	if err := w.WaitInit(ctx); err != nil {
		return nil, err
	}

	return w, nil
}

// WriteKeyedDocument demonstrates keyed ordering and publisher deduplication together.
func WriteKeyedDocument(ctx context.Context, w *topicwriter.Writer, key string, payload []byte) error {
	return w.Write(ctx, topicwriter.Message{
		Key:  key,
		Data: bytes.NewReader(payload),
	})
}

// WriteKeyedDocumentTx matches WriteKeyedDocument for transaction-scoped writers.
func WriteKeyedDocumentTx(ctx context.Context, w *topicwriter.TxWriter, key string, payload []byte) error {
	return w.Write(ctx, topicwriter.Message{
		Key:  key,
		Data: bytes.NewReader(payload),
	})
}

// WritePinnedPartition pairs with StartWriterAcrossPartitionsExplicitID and supplies Message.PartitionID explicitly.
func WritePinnedPartition(ctx context.Context, w *topicwriter.Writer, partitionID int64, payload []byte) error {
	return w.Write(ctx, topicwriter.Message{
		PartitionID: partitionID,
		Data:        bytes.NewReader(payload),
	})
}
