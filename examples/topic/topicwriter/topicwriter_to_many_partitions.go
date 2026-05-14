// This file illustrates how to configure a writer that sends data to multiple topic partitions behind a single API.
//
// Behavior that matters for applications:
//
// Ordering on read is per partitioning key: consumers see producer send order, including after splits relocate the same
// logical key onto another partition than earlier publishes. Stable producer IDs and monotonic sequence numbers mean
// retries do not visibly reorder deliveries.
//
// Exactly-once: YDB persists each message under a logical producer identifier and sequence number pair; duplicates with
// the same pair are suppressed. Prefix all writers with WithProducerIDPrefix (or configure producer IDs explicitly) and
// keep sequence numbers strictly increasing per producer partition session (automatic assignment is easiest).
//
// Auto-split partitions: Topics can scale out with server-side partitioning.
// Bound-style key routing reacts to splits by taking updated partition boundaries from DescribeTopic; keyed messages
// map to disjoint ranges so new partitions are picked up as they appear without changing producer code.
//
// Experimental: Writing to many partitions is experimental in the SDK; see VERSIONING.md.
//
// The load-focused binary under ../topicmultiwriter complements this snippet.
// Integration scenarios live in topic tests.
//
// Production applications only expose the topicoptions knobs; internal testing hooks (writers factory swaps,
// custom server chooser wiring without going through Bound/Kafka wrappers) intentionally stay SDK-private.
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
func StartWriterAcrossPartitionsBoundedKey(
	ctx context.Context,
	db *ydb.Driver,
	topicPath string,
) (*topicwriter.Writer, error) {
	return db.Topic().StartWriter(topicPath,
		topicoptions.WithWriteToManyPartitions(
			// BoundPartitionChooser places messages by comparing a hashed partitioning key against ordered partition bounds.
			// It is compatible with Topic auto partitioning that grows partition count over time—the client refreshes bounds.
			topicoptions.WithWriterPartitionByKey(topicoptions.BoundPartitionChooser(
				// Optional: customize how raw Message.Key becomes the string matched against boundaries.
				// The default hashes like legacy C++/Go producers; override only to align routing with upstream systems.
				topicoptions.WithBoundPartitionChooserPartitioningKeyHasher(func(key string) string { return key }),
			)),

			// ProducerIDPrefix works like a regular producer ID: the server uses it for deduplication.
			// Do not run two writers with the same prefix at the same time. If their partitions
			// overlap, the server returns an error and one of the writers stops.
			topicoptions.WithProducerIDPrefix("docs-example-bounded"),
			// Keep idle partition writers alive longer so sparse writes to the same
			// partition can reuse the existing internal writer instead of recreating it.
			topicoptions.WithWriterIdleTimeout(45*time.Minute),
		),
		// WithWriterWaitServerAck enforces acknowledgement before Write returns—a common choice for transactional safety.
		topicoptions.WithWriterWaitServerAck(true),
		// WithWriterSetAutoSeqNo assigns strictly increasing sequence numbers per producer/stream automatically.
		topicoptions.WithWriterSetAutoSeqNo(true),
	)
}

// KafkaCompatibleKeyRoutingOptions swaps Bound routing for Kafka-style Murmur hashing over Message.Key.
//
// Prefer when migrating Kafka producers and using hash % partitionCount instead of server numeric bounds.
// The partition list still refreshes during auto-split; hashing stays compatible with Kafka partitioning math.
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
func StartWriterAcrossPartitionsExplicitID(
	ctx context.Context,
	db *ydb.Driver,
	topicPath string,
) (*topicwriter.Writer, error) {
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

func WriteToPartitionID(ctx context.Context, db *ydb.Driver, topicPath string, partitionID int64) error {
	writer, err := db.Topic().StartWriter(
		topicPath,
		topicoptions.WithWriteToManyPartitions(
			// Writing to an explicit partition ID is rarely needed. Prefer partitioning
			// by key unless the application has a strong reason to choose partitions itself.
			//
			// Explicit partition IDs are harder to handle when the topic is split: writing
			// to a closed partition stops the whole writer, so the application must detect
			// the new partition layout and recreate the writer with a valid partition ID.
			topicoptions.WithWriterPartitionByPartitionID(),
			// ProducerIDPrefix works like a regular producer ID: the server uses it for deduplication.
			// Do not run two writers with the same prefix at the same time. If their partitions
			// overlap, the server returns an error and one of the writers stops.
			topicoptions.WithProducerIDPrefix("docs-example-partition-id"),
		),
	)
	if err != nil {
		return err
	}
	defer func() { _ = writer.Close(context.Background()) }()

	return writer.Write(ctx, topicwriter.Message{
		Data:        bytes.NewReader([]byte("message")),
		PartitionID: partitionID,
	})
}
