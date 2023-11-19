package topicwriterinternal

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xatomic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var errSingleStreamWriterDoubleClose = xerrors.Wrap(errors.New("ydb: single stream writer impl double closed"))

type SingleStreamWriterConfig struct {
	WritersCommonConfig

	stream                RawTopicWriterStream
	queue                 *messageQueue
	encodersMap           *EncoderMap
	getAutoSeq            bool
	reconnectorInstanceID string
}

func newSingleStreamWriterConfig(
	common WritersCommonConfig, //nolint:gocritic
	stream RawTopicWriterStream,
	queue *messageQueue,
	encodersMap *EncoderMap,
	getAutoSeq bool,
	reconnectorID string,
) SingleStreamWriterConfig {
	return SingleStreamWriterConfig{
		WritersCommonConfig:   common,
		stream:                stream,
		queue:                 queue,
		encodersMap:           encodersMap,
		getAutoSeq:            getAutoSeq,
		reconnectorInstanceID: reconnectorID,
	}
}

type SingleStreamWriter struct {
	ReceivedLastSeqNum int64
	SessionID          string
	PartitionID        int64
	CodecsFromServer   rawtopiccommon.SupportedCodecs
	Encoder            EncoderSelector

	cfg            SingleStreamWriterConfig
	allowedCodecs  rawtopiccommon.SupportedCodecs
	background     background.Worker
	closed         xatomic.Bool
	closeReason    error
	closeCompleted empty.Chan
}

func NewSingleStreamWriter(
	ctxForPProfLabelsOnly context.Context,
	cfg SingleStreamWriterConfig, //nolint:gocritic
) (*SingleStreamWriter, error) {
	res := newSingleStreamWriterStopped(ctxForPProfLabelsOnly, cfg)
	err := res.initStream()
	if err != nil {
		_ = res.close(context.Background(), err)
		return nil, err
	}
	res.start()
	return res, nil
}

func newSingleStreamWriterStopped(
	ctxForPProfLabelsOnly context.Context,
	cfg SingleStreamWriterConfig, //nolint:gocritic
) *SingleStreamWriter {
	return &SingleStreamWriter{
		cfg:            cfg,
		background:     *background.NewWorker(xcontext.WithoutDeadline(ctxForPProfLabelsOnly)),
		closeCompleted: make(empty.Chan),
	}
}

func (w *SingleStreamWriter) close(ctx context.Context, reason error) error {
	if !w.closed.CompareAndSwap(false, true) {
		return xerrors.WithStackTrace(errSingleStreamWriterDoubleClose)
	}

	defer close(w.closeCompleted)
	w.closeReason = reason

	resErr := w.cfg.stream.CloseSend()
	bgWaitErr := w.background.Close(ctx, reason)
	if resErr == nil {
		resErr = bgWaitErr
	}

	return resErr
}

func (w *SingleStreamWriter) WaitClose(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.closeCompleted:
		return w.closeReason
	}
}

func (w *SingleStreamWriter) start() {
	w.background.Start("topic writer update token", w.updateTokenLoop)
	w.background.Start("topic writer send messages", w.sendMessagesFromQueueToStreamLoop)
	w.background.Start("topic writer receive messages", w.receiveMessagesLoop)
}

func (w *SingleStreamWriter) initStream() (err error) {
	traceOnDone := trace.TopicOnWriterInitStream(w.cfg.tracer, w.cfg.reconnectorInstanceID, w.cfg.topic, w.cfg.producerID)
	defer traceOnDone(w.SessionID, err)

	req := w.createInitRequest()
	if err = w.cfg.stream.Send(&req); err != nil {
		return err
	}
	recvMessage, err := w.cfg.stream.Recv()
	if err != nil {
		return err
	}
	result, ok := recvMessage.(*rawtopicwriter.InitResult)
	if !ok {
		return xerrors.WithStackTrace(
			fmt.Errorf("ydb: failed init response message type: %v", reflect.TypeOf(recvMessage)),
		)
	}

	w.allowedCodecs = calculateAllowedCodecs(w.cfg.forceCodec, w.cfg.encodersMap, result.SupportedCodecs)
	if len(w.allowedCodecs) == 0 {
		return xerrors.WithStackTrace(errNoAllowedCodecs)
	}

	w.Encoder = NewEncoderSelector(
		w.cfg.encodersMap,
		w.allowedCodecs,
		w.cfg.compressorCount,
		w.cfg.tracer,
		w.cfg.reconnectorInstanceID,
		w.SessionID,
	)

	w.SessionID = result.SessionID
	w.ReceivedLastSeqNum = result.LastSeqNo
	w.PartitionID = result.PartitionID
	w.CodecsFromServer = result.SupportedCodecs
	return nil
}

func (w *SingleStreamWriter) createInitRequest() rawtopicwriter.InitRequest {
	return rawtopicwriter.InitRequest{
		Path:             w.cfg.topic,
		ProducerID:       w.cfg.producerID,
		WriteSessionMeta: w.cfg.writerMeta,
		Partitioning:     w.cfg.defaultPartitioning,
		GetLastSeqNo:     w.cfg.getAutoSeq,
	}
}

func (w *SingleStreamWriter) receiveMessagesLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		mess, err := w.cfg.stream.Recv()
		if err != nil {
			err = xerrors.WithStackTrace(fmt.Errorf("ydb: failed to receive message from write stream: %w", err))
			_ = w.close(ctx, err)
			return
		}

		switch m := mess.(type) {
		case *rawtopicwriter.WriteResult:
			if err = w.cfg.queue.AcksReceived(m.Acks); err != nil {
				reason := xerrors.WithStackTrace(err)
				closeCtx, closeCtxCancel := xcontext.WithCancel(ctx)
				closeCtxCancel()
				_ = w.close(closeCtx, reason)
				return
			}
		case *rawtopicwriter.UpdateTokenResponse:
			// pass
		default:
			trace.TopicOnWriterReadUnknownGrpcMessage(
				w.cfg.tracer,
				w.cfg.reconnectorInstanceID,
				w.SessionID,
				xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
					"ydb: unexpected message type in stream reader: %v",
					reflect.TypeOf(m),
				))),
			)
		}
	}
}

func (w *SingleStreamWriter) sendMessagesFromQueueToStreamLoop(ctx context.Context) {
	for {
		messages, err := w.cfg.queue.GetMessagesForSend(ctx)
		if err != nil {
			_ = w.close(ctx, err)
			return
		}

		targetCodec, err := w.Encoder.CompressMessages(messages)
		if err != nil {
			_ = w.close(ctx, err)
			return
		}

		onSentComplete := trace.TopicOnWriterSendMessages(
			w.cfg.tracer,
			w.cfg.reconnectorInstanceID,
			w.SessionID,
			targetCodec.ToInt32(),
			messages[0].SeqNo,
			len(messages),
		)
		err = sendMessagesToStream(w.cfg.stream, targetCodec, messages)
		onSentComplete(err)
		if err != nil {
			err = xerrors.WithStackTrace(fmt.Errorf("ydb: error send message to topic stream: %w", err))
			_ = w.close(ctx, err)
			return
		}
	}
}

func (w *SingleStreamWriter) updateTokenLoop(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	ticker := w.cfg.clock.NewTicker(w.cfg.credUpdateInterval)
	defer ticker.Stop()

	ctxDone := ctx.Done()
	tickerChan := ticker.Chan()
	for {
		select {
		case <-ctxDone:
			return
		case <-tickerChan:
			_ = w.sendUpdateToken(ctx)
		}
	}
}

func (w *SingleStreamWriter) sendUpdateToken(ctx context.Context) (err error) {
	token, err := w.cfg.cred.Token(ctx)
	if err != nil {
		return err
	}

	stream := w.cfg.stream
	if stream == nil {
		// not connected yet
		return nil
	}

	req := &rawtopicwriter.UpdateTokenRequest{}
	req.Token = token
	return stream.Send(req)
}
