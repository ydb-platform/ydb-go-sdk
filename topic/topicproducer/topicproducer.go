package topicproducer

import (
	"context"

	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicproducer"
)

// Message is a message to be written by Producer.
// It extends internal topic writer message with key, partition and ack callback.
type Message = internal.Message

// ProducerConfig defines configuration for Producer.
type ProducerConfig = internal.ProducerConfig

// KeyHasher is a function that transforms a key before partition selection.
type KeyHasher = internal.KeyHasher

// ChoosePartitionFunc is a custom partition selection function.
type ChoosePartitionFunc = internal.ChoosePartitionFunc

// PartitionChooserStrategy is a strategy for choosing a partition for a message.
// It is used to determine which partition a message should be written to.
// The default strategy is PartitionChooserStrategyBound.
type PartitionChooserStrategy = internal.PartitionChooserStrategy

// WriteStats is a struct that contains statistics about the write operations:
// - MessagesWritten is the number of messages written to the topic.
// - LastWrittenSeqNo is the last sequence number written to the topic.
type WriteStats = internal.WriteStats

const (
	// PartitionChooserStrategyBound is a strategy for choosing a partition for a message based on the bounds of each partition.
	// Key will be hashed using KeyHasher and after that the hash will be compared with the bounds of each partition.
	// The partition with the bounds that contain the hash will be chosen.
	PartitionChooserStrategyBound PartitionChooserStrategy = iota
	// PartitionChooserStrategyHash is a strategy for choosing a partition for a message based on the hash of the key.
	// Key will be hashed using KeyHasher and the hash will be used to choose the partition.
	PartitionChooserStrategyHash
)

var (
	// ErrAlreadyClosed is returned when Producer is closed more than once.
	ErrAlreadyClosed = internal.ErrAlreadyClosed
	// ErrNoSeqNo is returned when sequence number is required but not provided.
	ErrNoSeqNo = internal.ErrNoSeqNo
	// ErrNoBounds is returned when partition has no bounds for bound-based chooser.
	ErrNoBounds = internal.ErrNoBounds
)

// Producer represents a high-level topic producer.
// It manages underlying writers, handles reconnections and buffering.
type Producer struct {
	inner *internal.Producer
}

// NewProducer creates a new Producer instance.
// It is a thin wrapper around internal topic producer implementation.
func NewProducer(inner *internal.Producer) *Producer {
	return &Producer{
		inner: inner,
	}
}

// Write sends messages using the underlying producer.
func (p *Producer) Write(ctx context.Context, messages ...Message) error {
	return p.inner.Write(ctx, messages...)
}

// Flush waits until all in-flight messages are acknowledged.
func (p *Producer) Flush(ctx context.Context) error {
	return p.inner.Flush(ctx)
}

// WaitInit waits until producer initialization is completed or an error occurs.
func (p *Producer) WaitInit(ctx context.Context) error {
	return p.inner.WaitInit(ctx)
}

// Close gracefully stops producer, flushing pending messages.
func (p *Producer) Close(ctx context.Context) error {
	return p.inner.Close(ctx)
}

// GetWriteStats returns statistics about the write operations.
func (p *Producer) GetWriteStats() WriteStats {
	return p.inner.GetWriteStats()
}
