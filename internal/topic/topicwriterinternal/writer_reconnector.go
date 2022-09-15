package topicwriterinternal

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"runtime"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xatomic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	errConnTimeout                          = xerrors.Wrap(errors.New("ydb: connection timeout"))
	errStopWriterReconnector                = xerrors.Wrap(errors.New("ydb: stop writer reconnector"))
	errCloseWriterReconnectorConnectionLoop = xerrors.Wrap(errors.New("ydb: close writer reconnector connection loop"))
	errCloseWriterReconnectorReconnect      = xerrors.Wrap(errors.New("ydb: stream writer reconnect"))
	errNonZeroSeqNo                         = xerrors.Wrap(errors.New("ydb: non zero seqno for auto set seqno mode"))
	errNonZeroCreatedAt                     = xerrors.Wrap(errors.New("ydb: non zero Message.CreatedAt and set auto fill created at option")) //nolint:lll
	errNoAllowedCodecs                      = xerrors.Wrap(errors.New("ydb: no allowed codecs for write to topic"))
)

type WriterReconnectorConfig struct {
	WritersCommonConfig

	Common                       config.Common
	AdditionalEncoders           map[rawtopiccommon.Codec]PublicCreateEncoderFunc
	Connect                      ConnectFunc
	WaitServerAck                bool
	AutoSetSeqNo                 bool
	AutoSetCreatedTime           bool
	OnWriterInitResponseCallback PublicOnWriterInitResponseCallback

	connectTimeout time.Duration
}

func newWriterReconnectorConfig(options ...PublicWriterOption) WriterReconnectorConfig {
	cfg := WriterReconnectorConfig{
		WritersCommonConfig: WritersCommonConfig{
			cred:               credentials.NewAnonymousCredentials(),
			credUpdateInterval: time.Hour,
			clock:              clockwork.NewRealClock(),
			compressorCount:    runtime.NumCPU(),
		},
		AutoSetSeqNo:       true,
		AutoSetCreatedTime: true,
	}
	if cfg.compressorCount == 0 {
		cfg.compressorCount = 1
	}

	for _, f := range options {
		f(&cfg)
	}

	if cfg.connectTimeout == 0 {
		cfg.connectTimeout = cfg.Common.OperationTimeout()
	}
	if cfg.connectTimeout == 0 {
		cfg.connectTimeout = timeutil.InfiniteDuration
	}

	return cfg
}

type WriterReconnector struct {
	cfg WriterReconnectorConfig

	queue                          messageQueue
	background                     background.Worker
	clock                          clockwork.Clock
	firstConnectionHandled         xatomic.Bool
	firstInitResponseProcessedChan empty.Chan
	writerInstanceID               string

	m           xsync.RWMutex
	sessionID   string
	lastSeqNo   int64
	encodersMap *EncoderMap
}

func newWriterReconnector(cfg WriterReconnectorConfig) *WriterReconnector {
	res := newWriterReconnectorStopped(cfg)
	res.start()
	return res
}

func newWriterReconnectorStopped(cfg WriterReconnectorConfig) *WriterReconnector {
	writerInstanceID, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	res := &WriterReconnector{
		cfg:                            cfg,
		queue:                          newMessageQueue(),
		clock:                          clockwork.NewRealClock(),
		lastSeqNo:                      -1,
		firstInitResponseProcessedChan: make(empty.Chan),
		encodersMap:                    NewEncoderMap(),
		writerInstanceID:               writerInstanceID.String(),
	}

	for codec, creator := range cfg.AdditionalEncoders {
		res.encodersMap.AddEncoder(codec, creator)
	}

	res.sessionID = "not-connected-" + writerInstanceID.String()

	return res
}

func (w *WriterReconnector) fillFields(messages []messageWithDataContent) error {
	var now time.Time

	for i := range messages {
		msg := &messages[i]

		// SetSeqNo
		if w.cfg.AutoSetSeqNo {
			if msg.SeqNo != 0 {
				return xerrors.WithStackTrace(errNonZeroSeqNo)
			}
			w.lastSeqNo++
			msg.SeqNo = w.lastSeqNo
		}

		// Set created time
		if w.cfg.AutoSetCreatedTime {
			if msg.CreatedAt.IsZero() {
				if now.IsZero() {
					now = w.clock.Now()
				}
				msg.CreatedAt = now
			} else {
				return xerrors.WithStackTrace(errNonZeroCreatedAt)
			}
		}
	}

	return nil
}

func (w *WriterReconnector) start() {
	name := fmt.Sprintf("writer %q", w.cfg.topic)
	w.background.Start(name+", sendloop", w.connectionLoop)
}

func (w *WriterReconnector) Write(ctx context.Context, messages []Message) error {
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

		if w.cfg.WaitServerAck {
			waiter, err = w.queue.AddMessagesWithWaiter(messagesSlice)
		} else {
			err = w.queue.AddMessages(messagesSlice)
		}
	})
	if err != nil {
		return err
	}

	if !w.cfg.WaitServerAck {
		return nil
	}

	return w.queue.Wait(ctx, waiter)
}

func (w *WriterReconnector) createMessagesWithContent(messages []Message) ([]messageWithDataContent, error) {
	res := make([]messageWithDataContent, 0, len(messages))
	for i := range messages {
		mess := newMessageDataWithContent(messages[i], w.encodersMap)
		res = append(res, mess)
	}

	var sessionID string
	w.m.WithRLock(func() {
		sessionID = w.sessionID
	})
	onCompressDone := trace.TopicOnWriterCompressMessages(
		w.cfg.tracer,
		w.writerInstanceID,
		sessionID,
		w.cfg.forceCodec.ToInt32(),
		messages[0].SeqNo,
		len(messages),
		trace.TopicWriterCompressMessagesReasonCompressDataOnWriteReadData,
	)

	targetCodec := w.cfg.forceCodec
	if targetCodec == rawtopiccommon.CodecUNSPECIFIED {
		targetCodec = rawtopiccommon.CodecRaw
	}
	err := readInParallelWithCodec(res, targetCodec, w.cfg.compressorCount)
	onCompressDone(err)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (w *WriterReconnector) Close(ctx context.Context) error {
	return w.close(ctx, xerrors.WithStackTrace(errStopWriterReconnector))
}

func (w *WriterReconnector) close(ctx context.Context, reason error) (resErr error) {
	onDone := trace.TopicOnWriterClose(w.cfg.tracer, w.writerInstanceID, reason)
	defer func() {
		onDone(resErr)
	}()

	resErr = w.queue.Close(reason)
	bgErr := w.background.Close(ctx, reason)
	if resErr == nil {
		resErr = bgErr
	}
	return resErr
}

func (w *WriterReconnector) connectionLoop(ctx context.Context) {
	doneCtx := ctx.Done()
	attempt := 0

	createStreamContext := func() (context.Context, xcontext.CancelErrFunc) {
		// need suppress parent context cancelation for flush buffer while close writer
		return xcontext.WithErrCancel(xcontext.WithoutDeadline(ctx))
	}

	//nolint:ineffassign,staticcheck
	streamCtx, streamCtxCancel := createStreamContext()

	defer func() {
		streamCtxCancel(xerrors.WithStackTrace(errCloseWriterReconnectorConnectionLoop))
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		streamCtxCancel(xerrors.WithStackTrace(errCloseWriterReconnectorReconnect))
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

		traceOnDone := trace.TopicOnWriterReconnect(
			w.cfg.tracer,
			w.writerInstanceID,
			w.cfg.topic,
			w.cfg.producerID,
			attempt,
		)

		stream, trackedErr := w.connectWithTimeout(streamCtx)
		traceOnDone(trackedErr)

		var writer *SingleStreamWriter
		if trackedErr == nil {
			attempt = 0
			w.queue.ResetSentProgress()
			writer, trackedErr = NewSingleStreamWriter(ctx, w.createWriterStreamConfig(stream))
		}
		if trackedErr == nil {
			w.onWriterChange(writer)
			trackedErr = writer.CloseWait(ctx)
		}
		w.onWriterChange(nil)

		if !topic.IsRetryableError(trackedErr) {
			closeCtx, cancel := context.WithCancel(ctx)
			cancel()
			_ = w.close(closeCtx, trackedErr)
			return
		}
		// next iteration
	}
}

func (w *WriterReconnector) needReceiveLastSeqNo() bool {
	res := w.cfg.AutoSetSeqNo && !w.firstConnectionHandled.Load()
	return res
}

func (w *WriterReconnector) connectWithTimeout(streamLifetimeContext context.Context) (RawTopicWriterStream, error) {
	connectCtx, connectCancel := context.WithCancel(streamLifetimeContext)

	type resT struct {
		stream RawTopicWriterStream
		err    error
	}
	resCh := make(chan resT, 1)

	go func() {
		defer func() {
			p := recover()
			if p != nil {
				resCh <- resT{
					stream: nil,
					err:    xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("ydb: panic while connect to topic writer: %+v", p))),
				}
			}
		}()

		stream, err := w.cfg.Connect(connectCtx)
		resCh <- resT{stream: stream, err: err}
	}()

	timer := time.NewTimer(w.cfg.connectTimeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		connectCancel()
		return nil, xerrors.WithStackTrace(errConnTimeout)
	case res := <-resCh:
		// force no cancel connect context - because it will break stream
		// context will cancel by cancel streamLifetimeContext while reconnect or stop connection
		_ = connectCancel
		return res.stream, res.err
	}
}

func (w *WriterReconnector) onWriterChange(writerStream *SingleStreamWriter) {
	isFirstInit := false
	w.m.WithLock(func() {
		if writerStream == nil {
			w.sessionID = ""
			return
		}
		w.sessionID = writerStream.SessionID

		if !w.firstConnectionHandled.CompareAndSwap(false, true) {
			return
		}
		defer close(w.firstInitResponseProcessedChan)
		isFirstInit = true

		if w.cfg.AutoSetSeqNo {
			w.lastSeqNo = writerStream.ReceivedLastSeqNum
		}
	})

	if isFirstInit {
		w.onWriterInitCallbackHandler(writerStream)
	}
}

func (w *WriterReconnector) onWriterInitCallbackHandler(writerStream *SingleStreamWriter) {
	if w.cfg.OnWriterInitResponseCallback != nil {
		info := PublicWithOnWriterConnectedInfo{
			LastSeqNo:        w.lastSeqNo,
			SessionID:        w.sessionID,
			PartitionID:      writerStream.PartitionID,
			CodecsFromServer: createPublicCodecsFromRaw(writerStream.CodecsFromServer),
		}

		if err := w.cfg.OnWriterInitResponseCallback(info); err != nil {
			_ = w.close(context.Background(), fmt.Errorf("OnWriterInitResponseCallback return error: %w", err))
		}
	}
}

func (w *WriterReconnector) waitFirstInitResponse(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if w.firstConnectionHandled.Load() {
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

func (w *WriterReconnector) createWriterStreamConfig(stream RawTopicWriterStream) SingleStreamWriterConfig {
	cfg := newSingleStreamWriterConfig(
		w.cfg.WritersCommonConfig,
		stream,
		&w.queue,
		w.encodersMap,
		w.needReceiveLastSeqNo(),
		w.writerInstanceID,
	)
	return cfg
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

func calculateAllowedCodecs(forceCodec rawtopiccommon.Codec, encoderMap *EncoderMap,
	serverCodecs rawtopiccommon.SupportedCodecs,
) rawtopiccommon.SupportedCodecs {
	if forceCodec != rawtopiccommon.CodecUNSPECIFIED {
		if serverCodecs.AllowedByCodecsList(forceCodec) && encoderMap.IsSupported(forceCodec) {
			return rawtopiccommon.SupportedCodecs{forceCodec}
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
		if encoderMap.IsSupported(codec) {
			res = append(res, codec)
		}
	}
	if len(res) == 0 {
		res = nil
	}
	return res
}

type ConnectFunc func(ctx context.Context) (RawTopicWriterStream, error)

func createPublicCodecsFromRaw(codecs rawtopiccommon.SupportedCodecs) []topictypes.Codec {
	res := make([]topictypes.Codec, len(codecs))
	for i, v := range codecs {
		res[i] = topictypes.Codec(v)
	}
	return res
}
