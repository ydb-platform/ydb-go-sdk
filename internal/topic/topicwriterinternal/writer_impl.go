package topicwriterinternal

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

var (
	errStopWriterImpl            = xerrors.Wrap(errors.New("ydb: stop writer impl"))
	errCloseWriterImplStreamLoop = xerrors.Wrap(errors.New("ydb: close writer impl stream loop"))
	errCloseWriterImplReconnect  = xerrors.Wrap(errors.New("ydb: stream writer reconnect"))
	errCloseWriterImplStopWork   = xerrors.Wrap(errors.New("ydb: stop work with writer stream"))
	errBadCodec                  = xerrors.Wrap(errors.New("ydb: internal error - bad codec for message"))
)

type writerImplConfig struct {
	connect             ConnectFunc
	producerID          string
	topic               string
	writerMeta          map[string]string
	defaultPartitioning rawtopicwriter.Partitioning
}

func NewWriterImplConfig(connect ConnectFunc, producerID, topic string, meta map[string]string, partitioning rawtopicwriter.Partitioning) writerImplConfig {
	return writerImplConfig{
		connect:             connect,
		producerID:          producerID,
		topic:               topic,
		writerMeta:          meta,
		defaultPartitioning: partitioning,
	}
}

type WriterImpl struct {
	cfg writerImplConfig

	queue      messageQueue
	background background.Worker
	clock      clockwork.Clock

	m                xsync.RWMutex
	allowedCodecsVal rawtopiccommon.SupportedCodecs
	sessionID        string
}

func NewWriterImpl(cfg writerImplConfig) *WriterImpl {
	res := newWriterImplStopped(cfg)
	res.start()
	return &res
}

func newWriterImplStopped(cfg writerImplConfig) WriterImpl {
	return WriterImpl{
		cfg:   cfg,
		queue: newMessageQueue(),
		clock: clockwork.NewRealClock(),
	}
}

func (w *WriterImpl) start() {
	panic("not implemented")
}

func (w *WriterImpl) Write(ctx context.Context, messages *messageWithDataContentSlice) (rawtopicwriter.WriteResult, error) {
	if err := w.background.CloseReason(); err != nil {
		return rawtopicwriter.WriteResult{}, xerrors.WithStackTrace(fmt.Errorf("ydb: writer is closed: %w", err))
	}

	if err := w.send(ctx, messages); err != nil {
		return rawtopicwriter.WriteResult{}, err
	}

	return rawtopicwriter.WriteResult{}, nil
}

func (w *WriterImpl) Close(ctx context.Context) error {
	return w.close(ctx, xerrors.WithStackTrace(errStopWriterImpl))
}

func (w *WriterImpl) close(ctx context.Context, reason error) error {
	return w.background.Close(ctx, reason)
}

func (w *WriterImpl) send(ctx context.Context, messages *messageWithDataContentSlice) error {
	return w.queue.AddMessages(messages)
}

func (w *WriterImpl) sendLoop(ctx context.Context) {
	doneCtx := ctx.Done()
	attempt := 0

	createStreamContext := func() (context.Context, xcontext.CancelErrFunc) {
		// need suppress parent context cancelation for flush buffer while close writer
		return xcontext.WithErrCancel(xcontext.WithoutDeadline(ctx))
	}

	//nolint:ineffassign,staticcheck
	streamCtx, streamCtxCancel := createStreamContext()

	defer func() {
		streamCtxCancel(xerrors.WithStackTrace(errCloseWriterImplStreamLoop))
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		streamCtxCancel(xerrors.WithStackTrace(errCloseWriterImplReconnect))
		streamCtx, streamCtxCancel = createStreamContext()

		attempt++
		// delay if reconnect
		if attempt > 1 {
			delay := backoff.Fast.Delay(attempt - 2)
			select {
			case <-doneCtx:
				return
			case <-w.clock.After(delay):
				// pass
			}
		}

		stream, err := w.connectWithTimeout(streamCtx)
		// TODO: trace
		if err != nil {
			if !topic.IsRetryableError(err) {
				_ = w.background.Close(ctx, err)
				return
			}
			continue
		}
		attempt = 0

		err = w.communicateWithServerThroughExistedStream(ctx, stream)
		if !topic.IsRetryableError(err) {
			_ = w.background.Close(ctx, err)
			return
		}
		// next iteration
	}
}

func (w *WriterImpl) connectWithTimeout(streamLifetimeContext context.Context) (RawTopicWriterStream, error) {
	// TODO: impl
	return w.cfg.connect(streamLifetimeContext)
}

func (w *WriterImpl) communicateWithServerThroughExistedStream(ctx context.Context, stream RawTopicWriterStream) error {
	ctx, cancel := xcontext.WithErrCancel(ctx)
	defer func() {
		_ = stream.CloseSend()
		cancel(xerrors.WithStackTrace(errCloseWriterImplStopWork))
	}()

	if err := w.initStream(stream); err != nil {
		return err
	}

	w.background.Start("topic writer receive messages", func(_ context.Context) {
		w.receiveMessages(ctx, stream, cancel)
	})

	return w.sendMessagesFromQueueToStream(ctx, stream)
}

func (w *WriterImpl) initStream(stream RawTopicWriterStream) error {
	req := w.createInitRequest()
	if err := stream.Send(&req); err != nil {
		return err
	}
	recvMessage, err := stream.Recv()
	if err != nil {
		return err
	}
	result, ok := recvMessage.(*rawtopicwriter.InitResult)
	if !ok {
		return xerrors.WithStackTrace(
			fmt.Errorf("ydb: failed init response message type: %v", reflect.TypeOf(recvMessage)),
		)
	}

	w.m.Lock()
	defer w.m.Unlock()

	w.allowedCodecsVal = result.SupportedCodecs
	w.sessionID = result.SessionID
	return nil
}

func (w *WriterImpl) createInitRequest() rawtopicwriter.InitRequest {
	return rawtopicwriter.InitRequest{
		Path:             w.cfg.topic,
		ProducerID:       w.cfg.producerID,
		WriteSessionMeta: w.cfg.writerMeta,
		Partitioning:     w.cfg.defaultPartitioning,
	}
}

func (w *WriterImpl) receiveMessages(ctx context.Context, stream RawTopicWriterStream, cancel xcontext.CancelErrFunc) {
	for {
		if ctx.Err() != nil {
			return
		}

		mess, err := stream.Recv()
		if err != nil {
			cancel(xerrors.WithStackTrace(fmt.Errorf("ydb: failed to receive message from write stream: %w", err)))
			return
		}

		switch m := mess.(type) {
		case *rawtopicwriter.WriteResult:
			if err = w.queue.AcksReceived(m.Acks); err != nil {
				reason := xerrors.WithStackTrace(err)
				_ = w.close(ctx, reason)
				cancel(reason)
				return
			}
		}
	}
}

func (w *WriterImpl) sendMessagesFromQueueToStream(ctx context.Context, stream RawTopicWriterStream) error {
	w.queue.ResetSentProgress()
	for {
		messages, err := w.queue.GetMessagesForSend(ctx)
		if err != nil {
			return err
		}

		err = sendMessagesToStream(stream, messages.m)
		if err != nil {
			return xerrors.WithStackTrace(fmt.Errorf("ydb: error send message to topic stream: %w", err))
		}
	}
}

func sendMessagesToStream(stream RawTopicWriterStream, messages []messageWithDataContent) error {
	if len(messages) == 0 {
		return nil
	}

	// optimization for avoid allocation in common way - when all messages has same codec
	messageGroups := [][]messageWithDataContent{messages}
	if !allMessagesHasSameBufCodec(messages) {
		messageGroups = splitMessagesByBufCodec(messages)
	}

	for _, messageGroup := range messageGroups {
		request, err := createWriteRequest(messageGroup)
		if err != nil {
			return err
		}
		err = stream.Send(&request)
		if err != nil {
			return xerrors.WithStackTrace(fmt.Errorf("ydb: failed send write request: %w", err))
		}
	}
	return nil
}

func allMessagesHasSameBufCodec(messages []messageWithDataContent) bool {
	if len(messages) <= 1 {
		return true
	}

	codec := messages[0].bufCodec
	for i := range messages {
		if messages[i].bufCodec != codec {
			return false
		}
	}

	return true
}

func splitMessagesByBufCodec(messages []messageWithDataContent) (res [][]messageWithDataContent) {
	if len(messages) == 0 {
		return nil
	}

	currentGroupStart := 0
	currentCodec := messages[0].bufCodec
	for i := range messages {
		if messages[i].bufCodec != currentCodec {
			res = append(res, messages[currentGroupStart:i:i])
			currentGroupStart = i
			currentCodec = messages[i].bufCodec
		}
	}
	res = append(res, messages[currentGroupStart:len(messages):len(messages)])
	return res
}

func createWriteRequest(messages []messageWithDataContent) (res rawtopicwriter.WriteRequest, err error) {
	res.Codec = rawtopiccommon.CodecRaw
	if len(messages) == 0 {
		return res, nil
	}

	res.Codec = messages[0].bufCodec
	res.Messages = make([]rawtopicwriter.MessageData, len(messages))
	for i := range messages {
		res.Messages[i], err = createRawMessageData(res.Codec, &messages[i])
		if err != nil {
			return res, err
		}
	}

	return res, nil
}

func createRawMessageData(
	codec rawtopiccommon.Codec,
	mess *messageWithDataContent,
) (res rawtopicwriter.MessageData, err error) {
	if mess.bufCodec != codec {
		return res, xerrors.WithStackTrace(errBadCodec)
	}

	res.CreatedAt = mess.CreatedAt
	res.SeqNo = mess.SeqNo

	switch {
	case mess.Partitioning.hasPartitionID:
		res.Partitioning.Type = rawtopicwriter.PartitioningPartitionID
		res.Partitioning.PartitionID = mess.Partitioning.partitionID
	case mess.Partitioning.messageGroupID != "":
		res.Partitioning.Type = rawtopicwriter.PartitioningMessageGroupID
		res.Partitioning.MessageGroupID = mess.Partitioning.messageGroupID
	default:
		// pass
	}

	res.UncompressedSize = mess.bufUncompressedSize
	res.Data = mess.buf.Bytes()
	return res, nil
}

type ConnectFunc func(ctx context.Context) (RawTopicWriterStream, error)
