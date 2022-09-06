package topicwriterinternal

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xatomic"
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
	errNonZeroSeqNo              = xerrors.Wrap(errors.New("ydb: non zero seqno for auto set seqno mode"))
)

type writerImplConfig struct {
	connect             ConnectFunc
	producerID          string
	topic               string
	writerMeta          map[string]string
	defaultPartitioning rawtopicwriter.Partitioning
	waitServerAck       bool
	autoSetSeqNo        bool
}

func newWriterImplConfig(options ...PublicWriterOption) writerImplConfig {
	cfg := writerImplConfig{
		autoSetSeqNo: true,
	}
	for _, f := range options {
		f(&cfg)
	}
	return cfg
}

type WriterImpl struct {
	cfg writerImplConfig

	queue                      messageQueue
	background                 background.Worker
	clock                      clockwork.Clock
	firstInitResponseProcessed xatomic.Bool

	m                              xsync.RWMutex
	allowedCodecsVal               rawtopiccommon.SupportedCodecs
	sessionID                      string
	lastSeqNo                      int64
	firstInitResponseProcessedChan empty.Chan
}

func newWriterImpl(cfg writerImplConfig) *WriterImpl {
	res := newWriterImplStopped(cfg)
	res.start()
	return &res
}

func newWriterImplStopped(cfg writerImplConfig) WriterImpl {
	return WriterImpl{
		cfg:                            cfg,
		queue:                          newMessageQueue(),
		clock:                          clockwork.NewRealClock(),
		lastSeqNo:                      -1,
		firstInitResponseProcessedChan: make(empty.Chan),
	}
}

func (w *WriterImpl) setSeqNumbers(messages *messageWithDataContentSlice) error {
	for i := range messages.m {
		msg := &messages.m[i]
		if msg.SeqNo != 0 {
			return xerrors.WithStackTrace(errNonZeroSeqNo)
		}
		w.lastSeqNo++
		msg.SeqNo = w.lastSeqNo
	}
	return nil
}

func (w *WriterImpl) start() {
	name := fmt.Sprintf("writer %q", w.cfg.topic)
	w.background.Start(name+", sendloop", w.sendLoop)
}

func (w *WriterImpl) Write(ctx context.Context, messages *messageWithDataContentSlice) error {
	if err := w.background.CloseReason(); err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: writer is closed: %w", err))
	}

	if err := w.waitFirstInitResponse(ctx); err != nil {
		return err
	}

	var err error
	var waiter *MessageQueueAckWaiter
	w.m.WithLock(func() {
		// need set numbers and add to queue atomically
		if w.cfg.autoSetSeqNo {
			err = w.setSeqNumbers(messages)
			if err != nil {
				return
			}
		}

		if w.cfg.waitServerAck {
			waiter, err = w.queue.AddMessagesWithWaiter(messages)
		} else {
			err = w.queue.AddMessages(messages)
		}
	})
	if err != nil {
		return err
	}

	if !w.cfg.waitServerAck {
		return nil
	}

	return w.queue.Wait(ctx, waiter)
}

func (w *WriterImpl) Close(ctx context.Context) error {
	return w.close(ctx, xerrors.WithStackTrace(errStopWriterImpl))
}

func (w *WriterImpl) close(ctx context.Context, reason error) error {
	resErr := w.queue.Close(reason)
	bgErr := w.background.Close(ctx, reason)
	if resErr == nil {
		resErr = bgErr
	}
	return resErr
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
			closeCtx, cancel := context.WithCancel(ctx)
			cancel()
			_ = w.close(closeCtx, err)
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
	if req.GetLastSeqNo {
		w.lastSeqNo = result.LastSeqNo
	}
	if w.firstInitResponseProcessed.CompareAndSwap(false, true) {
		close(w.firstInitResponseProcessedChan)
	}
	return nil
}

func (w *WriterImpl) createInitRequest() rawtopicwriter.InitRequest {
	getLastSeqNo := w.lastSeqNo < 0 && w.cfg.autoSetSeqNo

	return rawtopicwriter.InitRequest{
		Path:             w.cfg.topic,
		ProducerID:       w.cfg.producerID,
		WriteSessionMeta: w.cfg.writerMeta,
		Partitioning:     w.cfg.defaultPartitioning,
		GetLastSeqNo:     getLastSeqNo,
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
				closeCtx, closeCtxCancel := context.WithCancel(ctx)
				closeCtxCancel()
				_ = w.close(closeCtx, reason)
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

func (w *WriterImpl) waitFirstInitResponse(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if w.firstInitResponseProcessed.Load() {
		return nil
	}

	select {
	case <-w.firstInitResponseProcessedChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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
