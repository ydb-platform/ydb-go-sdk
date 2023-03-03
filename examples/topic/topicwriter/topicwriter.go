package topicwriter

import (
	"bytes"
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

const groupID = "group-id"

func ConnectSimple(ctx context.Context, db *ydb.Driver) *topicwriter.Writer {
	writer, _ := db.Topic().StartWriter("topicName")
	return writer
}

func ConnectWithSyncWrite(ctx context.Context, db *ydb.Driver) *topicwriter.Writer {
	writer, _ := db.Topic().StartWriter("topicName", topicoptions.WithSyncWrite(true))
	return writer
}

func ConnectSelectCodec(ctx context.Context, db *ydb.Driver) *topicwriter.Writer {
	writer, _ := db.Topic().StartWriter("topicName", topicoptions.WithCodec(topictypes.CodecGzip))
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
