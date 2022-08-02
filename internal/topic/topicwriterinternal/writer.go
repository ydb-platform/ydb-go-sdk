package topicwriterinternal

import (
	"context"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
)

//nolint:lll
//go:generate mockgen -destination raw_topic_writer_stream_mock_test.go -package topicwriterinternal -write_package_comment=false github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal RawTopicWriterStream

type RawTopicWriterStream interface {
	Recv() (rawtopicwriter.ServerMessage, error)
	Send(mess rawtopicwriter.ClientMessage) error
	CloseSend() error
}

type Writer struct {
	streamWriter streamWriter
	clockwork.Clock
}

func (w *Writer) Write(ctx context.Context, messages ...Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	messagesWithContent := newContentMessagesSlice()

	for i := range messages {
		contentMessage, err := newMessageDataWithContent(messages[i])
		if err != nil {
			return err
		}
		messagesWithContent.m = append(messagesWithContent.m, contentMessage)
	}

	_, err := w.streamWriter.Write(ctx, messagesWithContent)
	return err
}

type WriteOptions struct {
	Codec     Codec
	Autocodec bool
}
