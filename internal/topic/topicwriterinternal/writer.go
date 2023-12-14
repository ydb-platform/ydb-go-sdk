package topicwriterinternal

import (
	"context"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
)

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

func NewWriter(cred credentials.Credentials, options []PublicWriterOption) (*Writer, error) {
	options = append(
		options,
		WithCredentials(cred),
	)
	cfg := newWriterReconnectorConfig(options...)
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	writerImpl := newWriterReconnector(cfg)

	return &Writer{
		streamWriter: writerImpl,
		clock:        clockwork.NewRealClock(),
	}, nil
}

func (w *Writer) Write(ctx context.Context, messages ...PublicMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return w.streamWriter.Write(ctx, messages)
}

func (w *Writer) WaitInit(ctx context.Context) (info InitialInfo, err error) {
	return w.streamWriter.WaitInit(ctx)
}

func (w *Writer) Close(ctx context.Context) error {
	return w.streamWriter.Close(ctx)
}
