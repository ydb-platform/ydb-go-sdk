package topicwriterinternal

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	errConnTimeout           = xerrors.Wrap(errors.New("ydb: connection timeout"))
	errStopWriterReconnector = xerrors.Wrap(errors.New("ydb: stop writer reconnector"))
	ErrNonZeroSeqNo          = xerrors.Wrap(errors.New("ydb: non zero seqno for auto set seqno mode"))
	errNoAllowedCodecs       = xerrors.Wrap(errors.New("ydb: no allowed codecs for write to topic"))
	errLargeMessage          = xerrors.Wrap(errors.New("ydb: message uncompressed size more, then limit"))
	ErrPublicQueueIsFull     = xerrors.Wrap(
		errors.New("ydb: queue is full"),
	)
	ErrPublicMessagesPutToInternalQueueBeforeError = xerrors.Wrap(errors.New("ydb: the messages was put to internal buffer before the error happened. It mean about the messages can be delivered to the server"))                                                                                                           //nolint:lll
	errDiffetentTransactions                       = xerrors.Wrap(errors.New("ydb: internal writer has messages from different trasactions. It is internal logic error, write issue please: https://github.com/ydb-platform/ydb-go-sdk/issues/new?assignees=&labels=bug&projects=&template=01_BUG_REPORT.md&title=bug%3A+")) //nolint:lll
	errWritingByKeyNotSupported                    = xerrors.Wrap(errors.New("ydb: writing by key is not supported for single writer, use WithWriterPartitionByKey or WithPartitionByPartitionID options"))                                                                                                                  //nolint:lll

	// errProducerIDNotEqualMessageGroupID is temporary
	// WithMessageGroupID is optional parameter because it allowed to be skipped by protocol.
	// But right not YDB server doesn't implement it.
	// It is fast check for return error at writer create context instead of stream initialization
	// The error will remove in the future, when skip message group id will be allowed by server.
	errProducerIDNotEqualMessageGroupID = xerrors.Wrap(errors.New("ydb: producer id not equal to message group id, use option WithMessageGroupID(producerID) for create writer")) //nolint:lll

	errDirectWritePartitionNotFound = xerrors.Wrap(errors.New("ydb: direct write: target partition not found in topic description")) //nolint:lll
	// errDirectWriteRebindAfterInit is a sentinel reconnect reason used when
	// direct write was enabled without a static partition ID. After the very
	// first InitResponse arrives we learn the partition the server assigned and
	// must reopen the stream bound to that partition's node — the reconnect
	// loop treats this sentinel as "rebind, do not pause".
	errDirectWriteRebindAfterInit = xerrors.Wrap(errors.New("ydb: direct write: rebind to assigned partition after first init"))
)

type WriterReconnectorConfig struct {
	WritersCommonConfig

	MaxMessageSize               int
	MaxQueueLen                  int
	Common                       config.Common
	AdditionalEncoders           map[rawtopiccommon.Codec]topicwritercommon.PublicCreateEncoderFunc
	Connect                      ConnectFunc
	WaitServerAck                bool
	AutoSetSeqNo                 bool
	AutoSetCreatedTime           bool
	OnWriterInitResponseCallback PublicOnWriterInitResponseCallback
	OnAckReceivedCallback        func(seqNo int64)
	MultiMode                    bool

	// ErrOnQueueFull controls Write behavior when the internal message queue is full.
	// false (default): Write blocks until queue space becomes available or ctx is cancelled.
	// true: Write returns ErrPublicQueueIsFull immediately without blocking,
	// allowing callers to implement back-pressure and avoid unbounded memory growth.
	ErrOnQueueFull bool

	MultiWriterConfig any
	RetrySettings     topic.RetrySettings

	// directWrite enables resolving the node that hosts the target partition
	// via DescribeTopic before each stream connect, and binds the gRPC call
	// to that node via endpoint.WithNodeID.
	//
	// If a static partition ID is supplied (defaultPartitioning.PartitionID),
	// it is used from the very first connect. Otherwise the first connect
	// goes through the proxy as usual; the partition the server assigns in
	// the InitResponse is then captured into directWriteResolvedPartitionID
	// and the stream is rebuilt against that partition's node.
	directWrite bool

	// directWriteResolvedPartitionID is a shared sink for the partition the
	// direct-write connect should bind to. -1 means "not yet known" and the
	// connect runs through the proxy. Stored via pointer so the connect
	// closure and the WriterReconnector see the same underlying atomic
	// regardless of how WriterReconnectorConfig is copied.
	directWriteResolvedPartitionID *atomic.Int64

	// directWritePartitionPinnedByUser is true when the caller pinned a
	// partition via WithWriterPartitionID. In that case the resolved-partition
	// atomic is set from config at start time and never reset — errors
	// propagate so the caller can react. When false, the partition is learned
	// from the server's first InitResponse; on any session failure we drop
	// the pin so the next connect re-discovers via the proxy. This covers
	// auto-partitioning (split / merge invalidates our partition) and node
	// migrations the SDK can't detect on its own.
	directWritePartitionPinnedByUser bool

	// directWriteOriginalPartitioning is the defaultPartitioning value as it
	// stood at config time, before the writer overwrote it with a learned
	// PartitioningPartitionID. We restore it whenever we drop the learned
	// partition so the next InitRequest goes back to the original routing
	// (typically PartitioningMessageGroupID derived from the producer ID).
	directWriteOriginalPartitioning rawtopicwriter.Partitioning

	connectTimeout time.Duration
}

func (cfg *WriterReconnectorConfig) validate() error {
	if cfg.defaultPartitioning.Type == rawtopicwriter.PartitioningMessageGroupID &&
		cfg.producerID != cfg.defaultPartitioning.MessageGroupID {
		return xerrors.WithStackTrace(errProducerIDNotEqualMessageGroupID)
	}

	return nil
}

func NewWriterReconnectorConfig(options ...PublicWriterOption) WriterReconnectorConfig {
	cfg := WriterReconnectorConfig{
		WritersCommonConfig: WritersCommonConfig{
			cred:               credentials.NewAnonymousCredentials(),
			credUpdateInterval: time.Hour,
			clock:              clockwork.NewRealClock(),
			compressorCount:    runtime.NumCPU(),
			Tracer:             &trace.Topic{},
			maxBytesPerMessage: config.DefaultGRPCMsgSize,
		},
		AutoSetSeqNo:       true,
		AutoSetCreatedTime: true,
		MaxMessageSize:     50 * 1024 * 1024, //nolint:mnd
		MaxQueueLen:        1000,             //nolint:mnd
		RetrySettings: topic.RetrySettings{
			StartTimeout: topic.DefaultStartTimeout,
		},
	}
	if cfg.compressorCount == 0 {
		cfg.compressorCount = 1
	}

	for _, f := range options {
		f(&cfg)
	}

	if cfg.LogContext == nil {
		cfg.LogContext = context.Background()
	}

	if cfg.connectTimeout == 0 {
		cfg.connectTimeout = cfg.Common.OperationTimeout()
	}

	if cfg.connectTimeout == 0 {
		cfg.connectTimeout = value.InfiniteDuration
	}

	if cfg.producerID == "" {
		WithProducerID(uuid.NewString())(&cfg)
	}

	// Seed the resolved-partition atomic. If the caller pinned a partition
	// via WithPartitioning(NewPartitioningWithPartitionID(...)) we can do
	// direct write from the very first connect; otherwise -1 signals
	// "ask the server in the first InitResponse, then rebind".
	cfg.directWriteResolvedPartitionID = &atomic.Int64{}
	cfg.directWriteOriginalPartitioning = cfg.defaultPartitioning
	if cfg.directWrite && cfg.defaultPartitioning.Type == rawtopicwriter.PartitioningPartitionID {
		cfg.directWriteResolvedPartitionID.Store(cfg.defaultPartitioning.PartitionID)
		cfg.directWritePartitionPinnedByUser = true
	} else {
		cfg.directWriteResolvedPartitionID.Store(-1)
	}

	if cfg.Connect == nil {
		cfg.Connect = defaultConnectFunc(&cfg)
	}

	return cfg
}

// defaultConnectFunc returns the ConnectFunc used when none is provided.
// When directWrite is enabled and the partition is known (either pinned by
// the caller or learned from a previous InitResponse), it pre-resolves the
// node hosting that partition via DescribeTopic and binds StreamWrite to it.
// When the partition is not yet known (-1) the call goes through the proxy
// as usual; the writer rebinds once the server reports the partition in the
// first InitResponse.
func defaultConnectFunc(cfg *WriterReconnectorConfig) ConnectFunc {
	return func(ctx context.Context, tracer *trace.Topic) (RawTopicWriterStream, error) {
		mergedCtx := xcontext.MergeContexts(ctx, cfg.LogContext)
		if cfg.directWrite {
			if partitionID := cfg.directWriteResolvedPartitionID.Load(); partitionID >= 0 {
				resolvedCtx, err := resolvePartitionNode(
					mergedCtx,
					cfg.rawTopicClient,
					cfg.topic,
					partitionID,
				)
				if err != nil {
					return nil, err
				}
				mergedCtx = resolvedCtx
			}
		}

		return cfg.rawTopicClient.StreamWrite(mergedCtx, tracer)
	}
}

// resolvePartitionNode looks up which node currently hosts the given partition
// of the topic and returns a context bound to that node via endpoint.WithNodeID.
// Returns the original DescribeTopic error wrapped via xerrors.WithStackTrace
// so the writer reconnect loop can classify it through topic.RetryDecision
// (UNAVAILABLE/OVERLOADED retry, BAD_REQUEST/SCHEME_ERROR stop).
func resolvePartitionNode(
	ctx context.Context,
	rawClient *rawtopic.Client,
	topicPath string,
	partitionID int64,
) (context.Context, error) {
	res, err := rawClient.DescribeTopic(ctx, rawtopic.DescribeTopicRequest{
		Path:            topicPath,
		IncludeLocation: true,
	})
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: direct write: describe topic failed: %w", err))
	}

	for i := range res.Partitions {
		p := &res.Partitions[i]
		if p.PartitionID == partitionID {
			nodeID := uint32(p.PartitionLocation.NodeID)
			// Temporary diagnostic: dump what we resolved so callers running
			// against unfamiliar clusters can tell whether the server is
			// honoring IncludeLocation. nodeID==0 usually means the server
			// returned an empty PartitionLocation.
			fmt.Fprintf(os.Stderr,
				"[direct-write] resolved topic=%q partition=%d → nodeID=%d generation=%d\n",
				topicPath, partitionID, nodeID, p.PartitionLocation.Generation,
			)
			return endpoint.WithNodeID(ctx, nodeID), nil
		}
	}

	return nil, xerrors.WithStackTrace(fmt.Errorf(
		"%w: topic=%q partition_id=%d",
		errDirectWritePartitionNotFound, topicPath, partitionID,
	))
}

type WriterReconnector struct {
	cfg                            WriterReconnectorConfig
	queue                          messageQueue
	background                     background.Worker
	retrySettings                  topic.RetrySettings
	writerInstanceID               string
	semaphore                      *xsync.SoftWeightedSemaphore
	firstInitResponseProcessedChan empty.Chan
	lastSeqNo                      int64
	encodersMap                    *topicwritercommon.MultiEncoder
	initDoneCh                     empty.Chan
	initInfo                       InitialInfo
	m                              xsync.RWMutex
	sessionID                      string
	firstConnectionHandled         atomic.Bool
	initDone                       bool
}

func NewWriterReconnector(
	cfg WriterReconnectorConfig,
) (*WriterReconnector, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	res := newWriterReconnectorStopped(cfg)
	res.start()

	return res, nil
}

func newWriterReconnectorStopped(
	cfg WriterReconnectorConfig, //nolint:gocritic
) *WriterReconnector {
	writerInstanceID, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	res := &WriterReconnector{
		cfg:                            cfg,
		semaphore:                      xsync.NewSoftWeightedSemaphore(int64(cfg.MaxQueueLen)),
		queue:                          newMessageQueue(),
		lastSeqNo:                      -1,
		firstInitResponseProcessedChan: make(empty.Chan),
		encodersMap:                    topicwritercommon.NewMultiEncoder(),
		writerInstanceID:               writerInstanceID.String(),
		retrySettings:                  cfg.RetrySettings,
	}

	res.queue.OnAckReceived = res.onAckReceived
	res.queue.AckCallback = res.cfg.OnAckReceivedCallback

	for codec, creator := range cfg.AdditionalEncoders {
		res.encodersMap.AddEncoder(codec, creator)
	}

	res.sessionID = "not-connected-" + writerInstanceID.String()

	res.initDoneCh = make(empty.Chan)

	return res
}

func (w *WriterReconnector) fillFields(messages []messageWithDataContent, preserveAssignedFields bool) error {
	for i := range messages {
		msg := &messages[i]

		// SetSeqNo
		if w.cfg.AutoSetSeqNo {
			if msg.SeqNo != 0 {
				if !preserveAssignedFields {
					return xerrors.WithStackTrace(ErrNonZeroSeqNo)
				}
			} else {
				w.lastSeqNo++
				msg.SeqNo = w.lastSeqNo
			}
		}
	}

	if w.cfg.AutoSetCreatedTime {
		if err := topicwritercommon.FillCreatedAt(messages, w.cfg.Now(), preserveAssignedFields); err != nil {
			return err
		}
	}

	return nil
}

func (w *WriterReconnector) start() {
	name := fmt.Sprintf("writer %q", w.cfg.topic)
	w.background.Start(name+", connectionLoop", w.connectionLoop)
}

func (w *WriterReconnector) Write(ctx context.Context, messages []PublicMessage) (resErr error) {
	if err := w.validateWriteMessages(messages); err != nil {
		return err
	}

	return w.writePrepared(ctx, len(messages), func() ([]topicwritercommon.MessageWithDataContent, error) {
		return w.createMessagesWithContent(messages)
	}, false)
}

func (w *WriterReconnector) WriteInternal(
	ctx context.Context,
	messages []topicwritercommon.MessageWithDataContent,
) (resErr error) {
	return w.writePrepared(ctx, len(messages), func() ([]topicwritercommon.MessageWithDataContent, error) {
		return messages, nil
	}, true)
}

func (w *WriterReconnector) validateWriteMessages(messages []PublicMessage) error {
	for i := range messages {
		if !w.cfg.MultiMode && (messages[i].Key != "" || messages[i].PartitionID != 0) {
			return xerrors.WithStackTrace(errWritingByKeyNotSupported)
		}
	}

	return nil
}

// acquireQueueSpaceForWrite reserves capacity in the internal message queue for weight messages.
// On success the caller must release the same weight with w.semaphore.Release (typically in defer).
func (w *WriterReconnector) acquireQueueSpaceForWrite(ctx context.Context, weight int64) error {
	if w.cfg.ErrOnQueueFull {
		// Non-blocking path: fail fast with ErrPublicQueueIsFull when the queue has no room.
		// This lets callers avoid unbounded memory growth and implement their own back-pressure.
		if !w.semaphore.TryAcquire(weight) {
			return xerrors.WithStackTrace(ErrPublicQueueIsFull)
		}

		return nil
	}

	if err := w.semaphore.Acquire(ctx, weight); err != nil {
		return xerrors.WithStackTrace(
			fmt.Errorf("ydb: timeout waiting for queue space to become available: %w", err),
		)
	}

	return nil
}

func (w *WriterReconnector) writePrepared(
	ctx context.Context,
	messageCount int,
	prepareMessages func() ([]topicwritercommon.MessageWithDataContent, error),
	preserveAssignedFields bool,
) (resErr error) {
	if err := w.background.CloseReason(); err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: writer is closed: %w", err))
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if messageCount == 0 {
		return nil
	}

	semaphoreWeight := int64(messageCount)
	if err := w.acquireQueueSpaceForWrite(ctx, semaphoreWeight); err != nil {
		return err
	}
	defer func() {
		w.semaphore.Release(semaphoreWeight)
	}()

	if err := w.waitFirstInitResponse(ctx); err != nil {
		return err
	}

	messages, err := prepareMessages()
	if err != nil {
		return err
	}

	return w.writeCommon(ctx, messages, &semaphoreWeight, preserveAssignedFields)
}

func (w *WriterReconnector) writeCommon(
	ctx context.Context,
	messages []topicwritercommon.MessageWithDataContent,
	semaphoreWeight *int64,
	preserveAssignedFields bool,
) (resErr error) {
	if err := w.checkMessages(messages); err != nil {
		return err
	}

	waiter, err := w.addMessageToInternalQueueWithLock(messages, semaphoreWeight, preserveAssignedFields)
	if err != nil {
		return err
	}
	defer func() {
		if resErr != nil {
			resErr = xerrors.Join(resErr, ErrPublicMessagesPutToInternalQueueBeforeError)
		}
	}()

	if !w.cfg.WaitServerAck {
		return nil
	}

	return w.queue.Wait(ctx, waiter)
}

func (w *WriterReconnector) addMessageToInternalQueueWithLock(
	messagesSlice []messageWithDataContent,
	semaphoreWeight *int64,
	preserveAssignedFields bool,
) (MessageQueueAckWaiter, error) {
	var (
		waiter MessageQueueAckWaiter
		err    error
	)
	w.m.WithLock(func() {
		// need set numbers and add to queue atomically
		err = w.fillFields(messagesSlice, preserveAssignedFields)
		if err != nil {
			return
		}

		if w.cfg.WaitServerAck {
			waiter, err = w.queue.AddMessagesWithWaiter(messagesSlice)
		} else {
			err = w.queue.AddMessages(messagesSlice)
		}
		if err == nil {
			// move semaphore weight to queue
			*semaphoreWeight = 0
		}
	})

	return waiter, err
}

func (w *WriterReconnector) checkMessages(messages []messageWithDataContent) error {
	for i := range messages {
		size := messages[i].BufUncompressedSize
		if size > w.cfg.MaxMessageSize {
			return xerrors.WithStackTrace(fmt.Errorf("message size bytes %v: %w", size, errLargeMessage))
		}
	}

	return nil
}

func (w *WriterReconnector) createMessagesWithContent(messages []PublicMessage) ([]messageWithDataContent, error) {
	res := make([]messageWithDataContent, 0, len(messages))
	for i := range messages {
		mess := newMessageDataWithContent(messages[i], w.encodersMap)
		res = append(res, mess)
	}

	var sessionID string
	w.m.WithRLock(func() {
		sessionID = w.sessionID
	})
	logCtx := w.cfg.LogContext
	onCompressDone := gtrace.TopicOnWriterCompressMessages(
		w.cfg.Tracer,
		&logCtx,
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
	err := topicwritercommon.CacheMessages(res, targetCodec, w.cfg.compressorCount)
	onCompressDone(err)

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (w *WriterReconnector) Flush(ctx context.Context) error {
	return w.queue.WaitLastWritten(ctx)
}

func (w *WriterReconnector) Close(ctx context.Context) error {
	reason := xerrors.WithStackTrace(errStopWriterReconnector)
	w.queue.StopAddNewMessages(reason)

	flushErr := w.Flush(ctx)
	closeErr := w.close(ctx, reason)

	if flushErr != nil {
		return flushErr
	}

	return closeErr
}

func (w *WriterReconnector) close(ctx context.Context, reason error) (resErr error) {
	defer func() {
		logCtx := w.cfg.LogContext
		gtrace.TopicOnWriterClose(w.cfg.Tracer, &logCtx, w.writerInstanceID, reason)
	}()

	// stop background work and single stream writer
	bgErr := w.background.Close(ctx, reason)
	if resErr == nil && bgErr != nil {
		resErr = bgErr
	}

	closeErr := w.queue.Close(reason)
	if resErr == nil && closeErr != nil {
		resErr = closeErr
	}

	return resErr
}

func (w *WriterReconnector) connectionLoop(ctx context.Context) {
	attempt := 0
	createStreamContext := func() (context.Context, context.CancelFunc) {
		// need suppress parent context cancelation for flush buffer while close writer
		return xcontext.WithCancel(xcontext.ValueOnly(ctx))
	}

	//nolint:ineffassign,staticcheck,wastedassign
	streamCtx, streamCtxCancel := createStreamContext()

	defer streamCtxCancel()

	var reconnectReason error
	var prevAttemptTime time.Time
	var startOfRetries time.Time

	for {
		if ctx.Err() != nil {
			return
		}

		streamCtxCancel()
		streamCtx, streamCtxCancel = createStreamContext()

		now := time.Now()
		if startOfRetries.IsZero() || topic.CheckResetReconnectionCounters(prevAttemptTime, now, w.cfg.connectTimeout) {
			attempt = 0
			startOfRetries = w.cfg.clock.Now()
		} else {
			attempt++
		}
		prevAttemptTime = now

		// Direct-write rebind isn't a real failure: we closed the stream
		// ourselves to switch from proxy mode to direct mode after learning
		// the partition. Skip the retry-pause / unretriable-error machinery.
		if errors.Is(reconnectReason, errDirectWriteRebindAfterInit) {
			reconnectReason = nil
			attempt = 0
			startOfRetries = time.Time{}
		}

		// Direct-write auto-partition rediscovery: when a session that was
		// bound to a server-learned partition fails for any reason, drop the
		// pin so the next connect goes back through the proxy and picks up
		// whatever partition the server now wants us to write to. This is
		// what makes us tolerant of auto-partitioning (a split / merge can
		// invalidate the partition we were holding) and of node migrations
		// where the previously hosting node no longer serves the partition.
		// User-pinned partitions are left untouched — the caller asked for
		// that specific partition and errors should propagate.
		if reconnectReason != nil &&
			w.cfg.directWrite &&
			!w.cfg.directWritePartitionPinnedByUser &&
			w.cfg.directWriteResolvedPartitionID.Load() >= 0 {
			w.cfg.directWriteResolvedPartitionID.Store(-1)
			w.m.WithLock(func() {
				w.cfg.defaultPartitioning = w.cfg.directWriteOriginalPartitioning
			})
		}

		if reconnectReason != nil {
			if w.handleReconnectRetry(ctx, reconnectReason, attempt, startOfRetries) {
				return
			}
		}

		logCtx := w.cfg.LogContext
		onWriterStarted := gtrace.TopicOnWriterReconnect(
			w.cfg.Tracer,
			&logCtx,
			w.writerInstanceID,
			w.cfg.topic,
			w.cfg.producerID,
			attempt,
		)

		writer, err := w.startWriteStream(streamCtx)
		w.onWriterChange(writer)
		onStreamError := onWriterStarted(err)
		if err == nil {
			reconnectReason = writer.WaitClose(ctx)
			startOfRetries = time.Now()
		} else {
			reconnectReason = err
		}
		onStreamError(reconnectReason)
	}
}

func (w *WriterReconnector) handleReconnectRetry(
	ctx context.Context,
	reconnectReason error,
	attempt int,
	startOfRetries time.Time,
) bool {
	retryDuration := w.cfg.clock.Since(startOfRetries)
	if backoff, stopRetryReason := topic.RetryDecision(
		reconnectReason,
		w.retrySettings,
		retryDuration,
	); stopRetryReason == nil {
		delay := backoff.Delay(attempt)
		delayTimer := w.cfg.clock.NewTimer(delay)
		select {
		case <-ctx.Done():
			delayTimer.Stop()

			return true
		case <-delayTimer.Chan():
			delayTimer.Stop() // no really need, stop for common style only
			// pass
		}
	} else {
		_ = w.close(ctx, fmt.Errorf("%w, was retried (%v)", stopRetryReason, retryDuration))

		return true
	}

	return false
}

func (w *WriterReconnector) startWriteStream(ctx context.Context) (writer *SingleStreamWriter, err error) {
	// connectCtx with timeout applies only to the connection phase,
	// allowing the main stream context to remain active after exiting this method
	connectCtx, stopConnectCtx := xcontext.WithStoppableTimeoutCause(ctx, w.cfg.connectTimeout, errConnTimeout)
	defer func() {
		// If the context was cancelled during connection (the stream was cancelled),
		// we should return a timeout error
		if !stopConnectCtx() && err == nil {
			err = context.Cause(connectCtx)
		}
	}()

	stream, err := w.connectWithTimeout(connectCtx)
	if err != nil {
		return nil, err
	}

	w.queue.ResetSentProgress()

	return NewSingleStreamWriter(connectCtx, w.createWriterStreamConfig(stream))
}

func (w *WriterReconnector) needReceiveLastSeqNo() bool {
	res := !w.firstConnectionHandled.Load()

	return res
}

func (w *WriterReconnector) connectWithTimeout(ctx context.Context) (stream RawTopicWriterStream, err error) {
	defer func() {
		p := recover()
		if p != nil {
			stream = nil
			err = xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("ydb: panic while connect to topic writer: %+v", p)))
		}
	}()

	return w.cfg.Connect(ctx, w.cfg.Tracer)
}

func (w *WriterReconnector) onAckReceived(count int) {
	w.semaphore.Release(int64(count))
}

func (w *WriterReconnector) onWriterChange(writerStream *SingleStreamWriter) {
	if writerStream == nil {
		w.m.WithLock(func() { w.sessionID = "" })

		return
	}

	var (
		isFirstInit   bool
		triggerRebind bool
	)
	w.m.WithLock(func() {
		w.sessionID = writerStream.SessionID

		// Direct-write rebind path. Triggered whenever the resolved-partition
		// atomic is -1 ("unknown"). This happens (a) on the very first
		// connect when the user did not pin a partition, and (b) after the
		// connection loop dropped a previously learned partition because the
		// session failed (auto-partitioning split / merge, node migration).
		//
		// We pin the partition the server just assigned, update both the
		// connect-time atomic (drives DescribeTopic-based node binding) and
		// cfg.defaultPartitioning (drives the next InitRequest, so the server
		// won't re-pick a different partition on a partition-specific node),
		// and tear down the current stream. The connectionLoop sentinel
		// `errDirectWriteRebindAfterInit` keeps the retry pause out of it.
		//
		// On the *very first* rebind we also keep firstInitResponseProcessedChan
		// open: user writes must wait until the rebound session is up, otherwise
		// the first batch would leak onto the proxy node we just left.
		if w.cfg.directWrite && w.cfg.directWriteResolvedPartitionID.Load() < 0 {
			pid := writerStream.PartitionID
			w.cfg.directWriteResolvedPartitionID.Store(pid)
			w.cfg.defaultPartitioning = rawtopicwriter.NewPartitioningPartitionID(pid)
			if writerStream.LastSeqNumRequested {
				w.lastSeqNo = writerStream.ReceivedLastSeqNum
			}
			triggerRebind = true

			return
		}

		if !w.firstConnectionHandled.CompareAndSwap(false, true) {
			return
		}
		defer close(w.firstInitResponseProcessedChan)
		isFirstInit = true

		if writerStream.LastSeqNumRequested {
			w.lastSeqNo = writerStream.ReceivedLastSeqNum
		}
	})

	if triggerRebind {
		// connectionLoop is about to call writerStream.WaitClose; closing
		// from this goroutine would deadlock. Hand off to a fresh goroutine.
		go func() {
			_ = writerStream.close(
				context.Background(),
				xerrors.WithStackTrace(errDirectWriteRebindAfterInit),
			)
		}()

		return
	}

	if isFirstInit {
		// Snapshot lastSeqNo/sessionID under the lock so the init callback can
		// read consistent values: once firstInitResponseProcessedChan closes
		// (deferred in the WithLock above), user.Write begins to mutate
		// w.lastSeqNo concurrently under the same mutex.
		var callbackLastSeqNo int64
		var callbackSessionID string
		w.m.WithLock(func() {
			w.initDone = true
			w.initInfo = InitialInfo{LastSeqNum: w.lastSeqNo}
			close(w.initDoneCh)
			callbackLastSeqNo = w.lastSeqNo
			callbackSessionID = w.sessionID
		})
		w.onWriterInitCallbackHandler(writerStream, callbackLastSeqNo, callbackSessionID)
	}
}

func (w *WriterReconnector) waitInit(ctx context.Context) (info InitialInfo, err error) {
	if ctx.Err() != nil {
		return InitialInfo{}, ctx.Err()
	}

	select {
	case <-ctx.Done():
		return InitialInfo{}, ctx.Err()
	case <-w.background.Done():
		return InitialInfo{}, w.background.CloseReason()
	case <-w.initDoneCh:
		return w.initInfo, nil
	}
}

func (w *WriterReconnector) WaitInit(ctx context.Context) error {
	_, err := w.waitInit(ctx)

	return err
}

func (w *WriterReconnector) WaitInitInfo(ctx context.Context) (info InitialInfo, err error) {
	return w.waitInit(ctx)
}

func (w *WriterReconnector) onWriterInitCallbackHandler(
	writerStream *SingleStreamWriter,
	lastSeqNo int64,
	sessionID string,
) {
	if w.cfg.OnWriterInitResponseCallback != nil {
		info := PublicWithOnWriterConnectedInfo{
			LastSeqNo:        lastSeqNo,
			SessionID:        sessionID,
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
	var ep trace.EndpointInfo
	if stream != nil {
		ep = stream.Endpoint() // endpoint.Endpoint implements trace.EndpointInfo
	}

	cfg := newSingleStreamWriterConfig(
		w.cfg.WritersCommonConfig,
		stream,
		&w.queue,
		w.encodersMap,
		w.needReceiveLastSeqNo(),
		w.writerInstanceID,
		ep,
	)

	return cfg
}

func (w *WriterReconnector) GetSessionID() (sessionID string) {
	w.m.WithLock(func() {
		sessionID = w.sessionID
	})

	return sessionID
}

func allMessagesHasSameBufCodec(messages []messageWithDataContent) bool {
	if len(messages) <= 1 {
		return true
	}

	codec := messages[0].BufCodec
	for i := range messages {
		if messages[i].BufCodec != codec {
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
	currentCodec := messages[0].BufCodec
	for i := range messages {
		if messages[i].BufCodec != currentCodec {
			res = append(res, messages[currentGroupStart:i:i])
			currentGroupStart = i
			currentCodec = messages[i].BufCodec
		}
	}
	res = append(res, messages[currentGroupStart:len(messages):len(messages)])

	return res
}

func createWriteRequest(messages []messageWithDataContent, targetCodec rawtopiccommon.Codec) (
	res *rawtopicwriter.WriteRequest,
	err error,
) {
	for i := 1; i < len(messages); i++ {
		if messages[i-1].Tx != messages[i].Tx {
			return nil, xerrors.WithStackTrace(errDiffetentTransactions)
		}
	}

	res = &rawtopicwriter.WriteRequest{}

	if len(messages) > 0 && messages[0].Tx != nil {
		res.Tx.ID = messages[0].Tx.ID()
		res.Tx.Session = messages[0].Tx.SessionID()
	}

	res.Codec = targetCodec
	res.Messages = make([]rawtopicwriter.MessageData, len(messages))
	for i := range messages {
		res.Messages[i], err = createRawMessageData(res.Codec, &messages[i])
		if err != nil {
			return nil, err
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
	case mess.FuturePartitioning.HasPartitionID:
		res.Partitioning.Type = rawtopicwriter.PartitioningPartitionID
		res.Partitioning.PartitionID = mess.FuturePartitioning.PartitionID
	case mess.FuturePartitioning.MessageGroupID != "":
		res.Partitioning.Type = rawtopicwriter.PartitioningMessageGroupID
		res.Partitioning.MessageGroupID = mess.FuturePartitioning.MessageGroupID
	default:
		// pass
	}

	res.UncompressedSize = int64(mess.BufUncompressedSize)
	res.Data, err = mess.GetEncodedBytes(codec)

	if len(mess.Metadata) > 0 {
		res.MetadataItems = make([]rawtopiccommon.MetadataItem, 0, len(mess.Metadata))
		for key, val := range mess.Metadata {
			res.MetadataItems = append(res.MetadataItems, rawtopiccommon.MetadataItem{
				Key:   key,
				Value: val,
			})
		}
	}

	return res, err
}

func calculateAllowedCodecs(forceCodec rawtopiccommon.Codec, multiEncoder *topicwritercommon.MultiEncoder,
	serverCodecs rawtopiccommon.SupportedCodecs,
) rawtopiccommon.SupportedCodecs {
	if forceCodec != rawtopiccommon.CodecUNSPECIFIED {
		if serverCodecs.AllowedByCodecsList(forceCodec) && multiEncoder.IsSupported(forceCodec) {
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
		if multiEncoder.IsSupported(codec) {
			res = append(res, codec)
		}
	}
	if len(res) == 0 {
		res = nil
	}

	return res
}

type ConnectFunc func(ctx context.Context, tracer *trace.Topic) (RawTopicWriterStream, error)

func createPublicCodecsFromRaw(codecs rawtopiccommon.SupportedCodecs) []topictypes.Codec {
	res := make([]topictypes.Codec, len(codecs))
	for i, v := range codecs {
		res[i] = topictypes.Codec(v)
	}

	return res
}
