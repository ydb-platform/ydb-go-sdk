package topicwriter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func ConnectSimple(ctx context.Context, db *ydb.Driver) *topicwriter.Writer {
	writer, _ := db.Topic().StartWriter("topicName")

	return writer
}

func ConnectWithSyncWrite(ctx context.Context, db *ydb.Driver) *topicwriter.Writer {
	writer, _ := db.Topic().StartWriter("topicName", topicoptions.WithWriterWaitServerAck(true))

	return writer
}

func ConnectSelectCodec(ctx context.Context, db *ydb.Driver) *topicwriter.Writer {
	writer, _ := db.Topic().StartWriter("topicName", topicoptions.WithWriterCodec(topictypes.CodecGzip))

	return writer
}

func SendMessagesOneByOne(ctx context.Context, w *topicwriter.Writer) {
	data := []byte{1, 2, 3}
	mess := topicwriter.Message{Data: bytes.NewReader(data)}
	_ = w.Write(ctx, mess)
}

func SendGroupOfMessages(ctx context.Context, w *topicwriter.Writer) {
	data1 := []byte{1, 2, 3}
	data2 := []byte{4, 5, 6}
	mess1 := topicwriter.Message{Data: bytes.NewReader(data1)}
	mess2 := topicwriter.Message{Data: bytes.NewReader(data2)}

	_ = w.Write(ctx, mess1, mess2)
}

func SendMessageToDedicatedPartition(ctx context.Context, w *topicwriter.Writer) {
	mess := topicwriter.Message{
		Data:         strings.NewReader("sdf"),
		Partitioning: topicwriter.ExplicitPartitionID(4),
	}
	_ = w.Write(ctx, mess)
}

func SendMessageWithMessageGroupID(ctx context.Context, db *ydb.Driver) {
	w, _ := db.Topic().StartWriter(
		"topic-path",

		// explicit enable - for force check
		topicoptions.WithWriterKeyID(true),

		// optional
		topicoptions.WithWriterMessageGroupHashFunc(func(messageGroupID string) [32]byte {
			return sha256.Sum256([]byte(messageGroupID))
		}),
	)

	// сохранение барьера по seqno, получение min/max seqno при реконнектах или старте автоинткрементного писателя

	// возможность писать одним producer_id в разные партиции

	mess := topicwriter.Message{
		Data:         strings.NewReader("sdf"),
		Partitioning: topicwriter.PartitionByKey("my-client"),
		Partitioning: topicwriter.ParitioningByKeyHash("", "dhdhdjdjdd"),
	}
	_ = w.Write(ctx, mess)
}
