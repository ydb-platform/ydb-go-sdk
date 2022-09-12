package topicwriterinternal

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"runtime"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
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
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	errStopWriterImpl            = xerrors.Wrap(errors.New("ydb: stop writer impl"))
	errCloseWriterImplStreamLoop = xerrors.Wrap(errors.New("ydb: close writer impl stream loop"))
	errCloseWriterImplReconnect  = xerrors.Wrap(errors.New("ydb: stream writer reconnect"))
	errCloseWriterImplStopWork   = xerrors.Wrap(errors.New("ydb: stop work with writer stream"))
	errNonZeroSeqNo              = xerrors.Wrap(errors.New("ydb: non zero seqno for auto set seqno mode"))
	errNoAllowedCodecs           = xerrors.Wrap(errors.New("ydb: no allowed codecs for write to topic"))
)

type writerImplConfig struct {
	tracer               trace.Topic
	connect              ConnectFunc
	cred                 credentials.Credentials
	credUpdateInterval   time.Duration
	producerID           string
	topic                string
	writerMeta           map[string]string
	defaultPartitioning  rawtopicwriter.Partitioning
	waitServerAck        bool
	autoSetSeqNo         bool
	additionalEncoders   map[rawtopiccommon.Codec]PublicCreateEncoderFunc
	forceCodec           rawtopiccommon.Codec
	fillEmptyCreatedTime bool
	compressorCount      int
	clock                clockwork.Clock
}

func newWriterImplConfig(options ...PublicWriterOption) writerImplConfig {
	cfg := writerImplConfig{
		autoSetSeqNo:         true,
		fillEmptyCreatedTime: true,
		cred:                 credentials.NewAnonymousCredentials(),
		credUpdateInterval:   time.Hour,
		compressorCount:      runtime.NumCPU(),
		clock:                clockwork.NewRealClock(),
	}
	if cfg.compressorCount == 0 {
		cfg.compressorCount = 1
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
	encoder                    EncoderSelector

	m                              xsync.RWMutex
	sessionID                      string
	lastSeqNo                      int64
	firstInitResponseProcessedChan empty.Chan
	encodersMap                    *EncoderMap
	streamVal                      RawTopicWriterStream
}

func newWriterImpl(cfg writerImplConfig) *WriterImpl {
	res := newWriterImplStopped(cfg)
	res.start()
	return res
}

func newWriterImplStopped(cfg writerImplConfig) *WriterImpl {
	res := &WriterImpl{
		cfg:                            cfg,
		queue:                          newMessageQueue(),
		clock:                          clockwork.NewRealClock(),
		lastSeqNo:                      -1,
		firstInitResponseProcessedChan: make(empty.Chan),
		encodersMap:                    NewEncoderMap(),
	}

	for codec, creator := range cfg.additionalEncoders {
		res.encodersMap.AddEncoder(codec, creator)
	}

	res.encoder = NewEncoderSelector(res.encodersMap, res.calculateAllowedCodecs(nil), cfg.compressorCount)
	id, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	res.sessionID = "not-connected-" + id.String()

	return res
}

func (w *WriterImpl) fillFields(messages []messageWithDataContent) error {
	var now time.Time

	for i := range messages {
		msg := &messages[i]

		// SetSeqNo
		if w.cfg.autoSetSeqNo {
			if msg.SeqNo != 0 {
				return xerrors.WithStackTrace(errNonZeroSeqNo)
			}
			w.lastSeqNo++
			msg.SeqNo = w.lastSeqNo
		}

		// Set created time
		if w.cfg.fillEmptyCreatedTime && msg.CreatedAt.IsZero() {
			if now.IsZero() {
				now = w.clock.Now()
			}
			msg.CreatedAt = now
		}
	}
	return nil
}

func (w *WriterImpl) stream() RawTopicWriterStream {
	w.m.RLock()
	defer w.m.RUnlock()

	return w.streamVal
}

func (w *WriterImpl) start() {
	name := fmt.Sprintf("writer %q", w.cfg.topic)
	w.background.Start(name+", sendloop", w.sendLoop)
}

func (w *WriterImpl) Write(ctx context.Context, messages []Message) error {
	if err := w.background.CloseReason(); err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: writer is closed: %w", err))
	}

	messagesSlice, err := w.createMessagesWithContent(messages)
	if err != nil {
		return err
	}

	if err = w.waitFirstInitResponse(ctx); err != nil {
		return err
	}

	var waiter MessageQueueAckWaiter
	w.m.WithLock(func() {
		// need set numbers and add to queue atomically
		err = w.fillFields(messagesSlice)
		if err != nil {
			return
		}

		if w.cfg.waitServerAck {
			waiter, err = w.queue.AddMessagesWithWaiter(messagesSlice)
		} else {
			err = w.queue.AddMessages(messagesSlice)
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

func (w *WriterImpl) createMessagesWithContent(messages []Message) ([]messageWithDataContent, error) {
	res := make([]messageWithDataContent, 0, len(messages))
	for i := range messages {
		mess, err := newMessageDataWithContent(messages[i], w.encodersMap, w.cfg.forceCodec)
		if err != nil {
			return nil, err
		}
		res = append(res, mess)
	}
	if err := compressMessages(res, w.cfg.forceCodec, w.cfg.compressorCount); err != nil {
		return nil, err
	}
	return res, nil
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

		traceOnDone := trace.TopicOnWriterReconnect(w.cfg.tracer, w.cfg.topic, w.cfg.producerID, w.sessionID, attempt)

		err := w.connectWithTimeout(streamCtx)
		traceOnDone(err)

		// TODO: trace
		if err != nil {
			if topic.IsRetryableError(err) {
				continue
			}
			_ = w.background.Close(ctx, err)
			return
		}
		attempt = 0

		err = w.communicateWithServerThroughExistedStream(ctx)
		if !topic.IsRetryableError(err) {
			closeCtx, cancel := context.WithCancel(ctx)
			cancel()
			_ = w.close(closeCtx, err)
			return
		}
		// next iteration
	}
}

func (w *WriterImpl) connectWithTimeout(streamLifetimeContext context.Context) error {
	// TODO: impl
	strm, err := w.cfg.connect(streamLifetimeContext)
	if err == nil {
		w.m.WithLock(func() {
			w.streamVal = strm
		})
	}

	return err
}

func (w *WriterImpl) communicateWithServerThroughExistedStream(ctx context.Context) error {
	traceOnDone := trace.TopicOnWriterInitStream(w.cfg.tracer, w.cfg.topic, w.cfg.producerID, w.sessionID)
	err := w.initStream()
	traceOnDone(err)
	if err != nil {
		return err
	}

	readStreamStopped := make(empty.Chan)
	tokenUpdaterStreamStopped := make(empty.Chan)
	defer func() {
		// wait reader
		select {
		case <-w.background.Done():
			// if background stopped - no new reader can be started
		case <-readStreamStopped:
			// wait reader stop for prevent reads in parallel
		}

		// wait token updater
		select {
		case <-w.background.Done():
			// if background stopped - no new reader can be started
		case <-tokenUpdaterStreamStopped:
			// wait reader stop for prevent reads in parallel
		}
	}()

	ctx, cancel := xcontext.WithErrCancel(ctx)
	defer func() {
		_ = w.stream().CloseSend()
		cancel(xerrors.WithStackTrace(errCloseWriterImplStopWork))
	}()

	w.background.Start("topic writer receive messages", func(_ context.Context) {
		defer close(readStreamStopped)
		w.receiveMessages(ctx, cancel)
	})

	w.background.Start("topic writer token updater", func(_ context.Context) {
		defer close(tokenUpdaterStreamStopped)
		w.updateTokenLoop(ctx)
	})

	return w.sendMessagesFromQueueToStream(ctx)
}

func (w *WriterImpl) initStream() error {
	req := w.createInitRequest()
	stream := w.stream()
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

	allowedCodecs := w.calculateAllowedCodecs(result.SupportedCodecs)
	if len(allowedCodecs) == 0 {
		return xerrors.WithStackTrace(errNoAllowedCodecs)
	}

	w.encoder.ResetAllowedCodecs(allowedCodecs)
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

func (w *WriterImpl) calculateAllowedCodecs(
	serverCodecs rawtopiccommon.SupportedCodecs,
) rawtopiccommon.SupportedCodecs {
	if w.cfg.forceCodec != rawtopiccommon.CodecUNSPECIFIED {
		if serverCodecs.AllowedByCodecsList(w.cfg.forceCodec) && w.encodersMap.IsSupported(w.cfg.forceCodec) {
			return rawtopiccommon.SupportedCodecs{w.cfg.forceCodec}
		}
		return nil
	}

	if len(serverCodecs) == 0 {
		// fixed list for autoselect codec if empty server list for prevent unexpectedly add messages with new codec
		// with sdk update
		serverCodecs = rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip}
	}

	res := make(rawtopiccommon.SupportedCodecs, 0, len(serverCodecs))
	for _, codec := range serverCodecs {
		if w.encodersMap.IsSupported(codec) {
			res = append(res, codec)
		}
	}
	if len(res) == 0 {
		res = nil
	}
	return res
}

func (w *WriterImpl) receiveMessages(ctx context.Context, cancel xcontext.CancelErrFunc) {
	for {
		if ctx.Err() != nil {
			return
		}

		mess, err := w.stream().Recv()
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
		case *rawtopicwriter.UpdateTokenResponse:
			// pass
		default:
			// TODO: trace
		}
	}
}

func (w *WriterImpl) sendMessagesFromQueueToStream(ctx context.Context) error {
	w.queue.ResetSentProgress()

	for {
		messages, err := w.queue.GetMessagesForSend(ctx)
		if err != nil {
			return err
		}

		targetCodec, err := w.encoder.CompressMessages(messages)
		if err != nil {
			return err
		}

		err = sendMessagesToStream(w.stream(), targetCodec, messages)
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
	case <-w.background.Done():
		return w.background.CloseReason()
	case <-w.firstInitResponseProcessedChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *WriterImpl) updateTokenLoop(ctx context.Context) {
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

func (w *WriterImpl) sendUpdateToken(ctx context.Context) (err error) {
	token, err := w.cfg.cred.Token(ctx)
	if err != nil {
		return err
	}

	stream := w.stream()
	if stream == nil {
		// not connected yet
		return nil
	}

	req := &rawtopicwriter.UpdateTokenRequest{}
	req.Token = token
	return stream.Send(req)
}

func sendMessagesToStream(
	stream RawTopicWriterStream,
	targetCodec rawtopiccommon.Codec,
	messages []messageWithDataContent,
) error {
	if len(messages) == 0 {
		return nil
	}

	request, err := createWriteRequest(messages, targetCodec)
	if err != nil {
		return err
	}
	err = stream.Send(&request)
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: failed send write request: %w", err))
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

func createWriteRequest(messages []messageWithDataContent, targetCodec rawtopiccommon.Codec) (
	res rawtopicwriter.WriteRequest,
	err error,
) {
	res.Codec = targetCodec
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
	res.Data, err = mess.GetEncodedBytes(codec)
	return res, err
}

type ConnectFunc func(ctx context.Context) (RawTopicWriterStream, error)
