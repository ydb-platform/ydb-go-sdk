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
	streamWriter StreamWriter
	clock        clockwork.Clock
}

func NewWriter(options []PublicWriterOption) *Writer {
	cfg := newWriterImplConfig(options...)
	writerImpl := newWriterImpl(cfg)

	return &Writer{
		streamWriter: writerImpl,
		clock:        clockwork.NewRealClock(),
	}
}

func (w *Writer) Write(ctx context.Context, messages ...Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return w.streamWriter.Write(ctx, messages)
}

func (w *Writer) Close(ctx context.Context) error {
	return w.streamWriter.Close(ctx)
}

type WriteOption struct {
	Codec     Codec
	Autocodec bool
}
