package topicreaderexamples

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// ReadMessagesWithCustomBatching example of custom of readed message batch
func ReadMessagesWithCustomBatching(ctx context.Context, db ydb.Connection) {
	reader, _ := db.Topic().StartReader("consumer", nil,
		topicoptions.WithBatchReadMinCount(1000),
	)

	for {
		batch, _ := reader.ReadMessageBatch(ctx)
		processBatch(batch.Context(), batch)
		_ = reader.Commit(batch.Context(), batch)
	}
}

// MyMessage example type with own serialization
type MyMessage struct {
	ID         byte
	ChangeType byte
	Delta      uint32
}

// UnmarshalYDBTopicMessage implements topicreader.MessageContentUnmarshaler interface
func (m *MyMessage) UnmarshalYDBTopicMessage(data []byte) error {
	if len(data) != 6 {
		return errors.New("bad data len")
	}
	m.ID = data[0]
	m.ChangeType = data[1]
	m.Delta = binary.BigEndian.Uint32(data[2:])
	return nil
}

// UnmarshalMessageContentToOwnType is example about effective unmarshal own format from message content
func UnmarshalMessageContentToOwnType(ctx context.Context, reader *topicreader.Reader) {
	var v MyMessage
	mess, _ := reader.ReadMessage(ctx)
	_ = mess.UnmarshalTo(&v)
}

// ProcessMessagesWithSyncCommit example about guarantee wait for commit accepted by server
func ProcessMessagesWithSyncCommit(ctx context.Context, db ydb.Connection) {
	reader, _ := db.Topic().StartReader("consumer", nil,
		topicoptions.WithCommitMode(topicoptions.CommitModeSync),
	)
	defer func() {
		_ = reader.Close(ctx)
	}()

	for {
		batch, _ := reader.ReadMessageBatch(ctx)
		processBatch(batch.Context(), batch)
		_ = reader.Commit(ctx, batch) // will wait response about commit from server
	}
}

// OwnReadProgressStorage example about store reading progress in external system and don't use
// commit messages to YDB
func OwnReadProgressStorage(ctx context.Context, db ydb.Connection) {
	reader, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
		topicoptions.WithGetPartitionStartOffset(
			func(
				ctx context.Context,
				req topicoptions.GetPartitionStartOffsetRequest,
			) (
				res topicoptions.GetPartitionStartOffsetResponse,
				err error,
			) {
				offset, err := readLastOffsetFromDB(ctx, req.Topic, req.PartitionID)
				res.StartFrom(offset)

				// Reader will stop if return err != nil
				return res, err
			}),
	)

	for {
		batch, _ := reader.ReadMessageBatch(ctx)

		processBatch(batch.Context(), batch)
		_ = externalSystemCommit(
			batch.Context(),
			batch.Topic(),
			batch.PartitionID(),
			getEndOffset(batch),
		)
	}
}
