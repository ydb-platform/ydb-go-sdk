package topiclistenerinternal

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// extractSelectorNames extracts topic names from selectors for tracing
func extractSelectorNames(selectors []*topicreadercommon.PublicReadSelector) []string {
	result := make([]string, len(selectors))
	for i, selector := range selectors {
		result[i] = selector.Path
	}

	return result
}

type streamListener struct {
	cfg *StreamListenerConfig

	stream      topicreadercommon.RawTopicReaderStream
	streamClose context.CancelCauseFunc
	handler     EventHandler
	sessionID   string
	listenerID  string

	background       background.Worker
	sessions         *topicreadercommon.PartitionSessionStorage
	sessionIDCounter *atomic.Int64

	hasNewMessagesToSend empty.Chan
	syncCommitter        *topicreadercommon.Committer

	freeBytes chan int

	closing atomic.Bool
	tracer  *trace.Topic

	shutdownInit       sync.Once
	shutdownOnce       sync.Once
	shutdownReasonOnce sync.Once
	shutdownDone       chan struct{}
	shutdownReason     error
	shutdownErr        error

	m              xsync.Mutex
	workers        map[rawtopicreader.PartitionSessionID]*PartitionWorker
	workerStates   map[*PartitionWorker]*partitionWorkerState
	messagesToSend []rawtopicreader.ClientMessage
}

type partitionWorkerState struct {
	worker       *PartitionWorker
	closeDone    chan struct{}
	closeStarted bool
	closeErr     error
}

type partitionWorkerCloseTask struct {
	state    *partitionWorkerState
	closeNow bool
}

func newStreamListener(
	connectionCtx context.Context,
	client TopicClient,
	eventListener EventHandler,
	config *StreamListenerConfig,
	sessionIDCounter *atomic.Int64,
) (*streamListener, error) {
	// Generate unique listener ID
	listenerIDRand, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		listenerIDRand = big.NewInt(-1)
	}
	listenerID := "listener-" + listenerIDRand.String()

	res := &streamListener{
		cfg:              config,
		handler:          eventListener,
		background:       *background.NewWorker(xcontext.ValueOnly(connectionCtx), "topic reader stream listener"),
		sessionIDCounter: sessionIDCounter,
		listenerID:       listenerID,

		tracer: config.Tracer,
	}

	res.initVars(sessionIDCounter)

	logCtx := connectionCtx
	initDone := gtrace.TopicOnListenerInit(
		res.tracer, &logCtx, res.listenerID, res.cfg.Consumer, extractSelectorNames(res.cfg.Selectors),
	)

	if err := res.initStream(connectionCtx, client); err != nil {
		initDone("", err)
		// Initialization may have created a stream and a stream-cancellation
		// watchdog even though the listener itself was not returned. Complete
		// that partial listener cleanup before allowing a reconnect attempt, but
		// keep the initialization error as the constructor result.
		_ = res.Close(context.Background(), err)

		return nil, err
	}

	initDone(res.sessionID, nil)

	res.syncCommitter = topicreadercommon.NewCommitterStopped(
		res.tracer,
		res.background.Context(),
		topicreadercommon.CommitModeSync,
		res.stream.Send,
	)

	res.startBackground()
	select {
	case res.freeBytes <- config.BufferSize:
	case <-res.background.Context().Done():
	}

	return res, nil
}

func (l *streamListener) Close(ctx context.Context, reason error) error {
	l.beginClose(ctx, reason)
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-l.shutdownDone:
		return l.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *streamListener) goClose(ctx context.Context, reason error) {
	l.beginClose(ctx, reason)
}

func (l *streamListener) beginClose(ctx context.Context, reason error) {
	l.shutdownInit.Do(func() {
		l.shutdownDone = make(chan struct{})
	})

	l.shutdownOnce.Do(func() {
		l.closing.Store(true)
		l.recordShutdownReason(reason)

		go func() {
			var resErrors []error
			logCtx := ctx
			closeDone := gtrace.TopicOnListenerClose(
				l.tracer,
				&logCtx,
				l.listenerID,
				l.sessionID,
				l.shutdownReason,
			)

			// Stop the read stream and wait for its receiver and sender loops before
			// taking the worker snapshot. This prevents a receiver from creating a
			// worker after the snapshot while shutdown is already in progress.
			if l.streamClose != nil {
				l.streamClose(l.shutdownReason)
			}

			// Shutdown owns this worker and must continue draining after the caller's
			// context expires. Partition callbacks are drained below with the same
			// detached lifetime.
			_ = l.background.Close(context.Background(), l.shutdownReason)

			// Claim all workers after the stream loops have stopped. A worker may
			// already have stopped naturally and been removed from the routing map;
			// its state remains retained until this close/join completes.
			workerTasks := l.claimWorkersForClose()
			for _, task := range workerTasks {
				if task.closeNow {
					l.finishWorkerClose(task.state, l.shutdownReason)
				}

				<-task.state.closeDone
				if task.state.closeErr != nil {
					resErrors = append(resErrors, task.state.closeErr)
				}
			}

			if l.syncCommitter != nil {
				_ = l.syncCommitter.Close(context.Background(), l.shutdownReason)
			}

			if l.sessions != nil {
				for _, session := range l.sessions.GetAll() {
					session.Close()
					// For shutdown, we don't need to process stop partition requests through workers
					// since all workers are already being closed above.
				}
			}

			l.shutdownErr = errors.Join(resErrors...)
			// The done hook is part of listener shutdown. Publish shutdownDone only
			// after it returns so WaitStop cannot report completion while tracing is
			// still running.
			closeDone(len(workerTasks), l.shutdownErr)
			close(l.shutdownDone)
		}()
	})
}

func (l *streamListener) claimWorkersForClose() []partitionWorkerCloseTask {
	var tasks []partitionWorkerCloseTask
	l.m.WithLock(func() {
		tasks = make([]partitionWorkerCloseTask, 0, len(l.workerStates))
		for _, state := range l.workerStates {
			closeNow := !state.closeStarted
			state.closeStarted = true
			tasks = append(tasks, partitionWorkerCloseTask{state: state, closeNow: closeNow})
		}
	})

	return tasks
}

func (l *streamListener) workerStateLocked(worker *PartitionWorker) *partitionWorkerState {
	if state, ok := l.workerStates[worker]; ok {
		return state
	}

	state := &partitionWorkerState{
		worker:    worker,
		closeDone: make(chan struct{}),
	}
	l.workerStates[worker] = state

	return state
}

func (l *streamListener) finishWorkerClose(state *partitionWorkerState, reason error) {
	state.closeErr = state.worker.Close(context.Background(), reason)
	l.m.WithLock(func() {
		if current, ok := l.workerStates[state.worker]; ok && current == state {
			delete(l.workerStates, state.worker)
		}
	})
	close(state.closeDone)
}

func (l *streamListener) closeWorkerAfterStop(state *partitionWorkerState, reason error) {
	go func() {
		l.finishWorkerClose(state, reason)
	}()
}

func (l *streamListener) startBackground() {
	l.background.Start("stream listener send loop", l.sendMessagesLoop)
	l.background.Start("stream listener receiver", l.receiveMessagesLoop)
	l.syncCommitter.Start()
}

func (l *streamListener) initVars(sessionIDCounter *atomic.Int64) {
	l.hasNewMessagesToSend = make(empty.Chan, 1)
	// A full channel applies backpressure until sendMessagesLoop drains it.
	l.freeBytes = make(chan int, 1)
	l.sessions = &topicreadercommon.PartitionSessionStorage{}
	l.sessionIDCounter = sessionIDCounter
	l.workers = make(map[rawtopicreader.PartitionSessionID]*PartitionWorker)
	l.workerStates = make(map[*PartitionWorker]*partitionWorkerState)
	if l.cfg == nil {
		l.cfg = &StreamListenerConfig{}
	}
}

//nolint:funlen
func (l *streamListener) initStream(ctx context.Context, client TopicClient) error {
	streamCtx, streamClose := context.WithCancelCause(xcontext.ValueOnly(ctx))
	l.streamClose = streamClose
	initDone := make(empty.Chan)
	defer close(initDone)

	go func() {
		select {
		case <-ctx.Done():
			err := xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
				"ydb: topic listener stream init timeout: %w", ctx.Err(),
			)))
			l.recordShutdownReason(err)
			l.streamClose(err)
		case <-initDone:
			// pass
		}
	}()

	stream, err := client.StreamRead(streamCtx, -1, l.tracer)
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: topic listener failed connect to a stream: %w",
			err,
		)))
	}
	l.stream = topicreadercommon.NewSyncedStream(stream)

	initMessage := topicreadercommon.CreateInitMessage(l.cfg.Consumer, false, l.cfg.Selectors)
	err = stream.Send(initMessage)
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: failed to send init request for read stream in the listener: %w", err)))
	}

	resp, err := l.stream.Recv()
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: failed to receive init response for read stream in the listener: %w",
			err,
		)))
	}

	if status := resp.StatusData(); !status.Status.IsSuccess() {
		// wrap initialization error as operation status error - for handle with retrier
		// https://github.com/ydb-platform/ydb-go-sdk/issues/1361
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: received bad status on init the topic stream listener: %v (%v)",
			status.Status,
			status.Issues,
		)))
	}

	initResp, ok := resp.(*rawtopicreader.InitResponse)
	if !ok {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"bad message type on session init: %v (%v)",
			resp,
			reflect.TypeOf(resp),
		)))
	}

	l.sessionID = initResp.SessionID

	return nil
}

func (l *streamListener) recordShutdownReason(reason error) {
	l.shutdownReasonOnce.Do(func() {
		l.shutdownReason = reason
	})
}

func (l *streamListener) sendMessagesLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case free := <-l.freeBytes:
			// Coalesce releases into one ReadRequest.
			sum := l.collectPendingFreeBytes(free)
			// We flush immediately, so there is no need to signal the send loop.
			l.m.WithLock(func() {
				l.messagesToSend = append(l.messagesToSend, &rawtopicreader.ReadRequest{BytesSize: sum})
			})
			l.flushPendingMessages(ctx)
		case <-l.hasNewMessagesToSend:
			l.flushPendingMessages(ctx)
		}
	}
}

func (l *streamListener) flushPendingMessages(ctx context.Context) {
	var messages []rawtopicreader.ClientMessage
	l.m.WithLock(func() {
		messages = l.messagesToSend
		if len(messages) > 0 {
			l.messagesToSend = make([]rawtopicreader.ClientMessage, 0, cap(messages))
		}
	})

	if len(messages) == 0 {
		return
	}

	logCtx := l.background.Context()

	for i, m := range messages {
		messageType := l.getMessageTypeName(m)

		if err := l.stream.Send(m); err != nil {
			// Trace send error
			l.traceMessageSend(&logCtx, messageType, err)
			gtrace.TopicOnListenerError(l.tracer, &logCtx, l.listenerID, l.sessionID, err)

			reason := xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
				"ydb: failed send message by grpc to topic reader stream from listener: "+
					"message_type=%s, message_index=%d, total_messages=%d: %w",
				messageType, i, len(messages), err,
			)))
			l.goClose(ctx, reason)

			return
		}

		// Trace successful send
		l.traceMessageSend(&logCtx, messageType, nil)
	}
}

// traceMessageSend provides consistent tracing for message sends
func (l *streamListener) traceMessageSend(ctx *context.Context, messageType string, err error) {
	// Use TopicOnListenerSendDataRequest for all message send tracing
	gtrace.TopicOnListenerSendDataRequest(l.tracer, ctx, l.listenerID, l.sessionID, messageType, err)
}

// getMessageTypeName returns a human-readable name for the message type
func (l *streamListener) getMessageTypeName(m rawtopicreader.ClientMessage) string {
	switch m.(type) {
	case *rawtopicreader.ReadRequest:
		return "ReadRequest"
	case *rawtopicreader.StartPartitionSessionResponse:
		return "StartPartitionSessionResponse"
	case *rawtopicreader.StopPartitionSessionResponse:
		return "StopPartitionSessionResponse"
	case *rawtopicreader.CommitOffsetRequest:
		return "CommitOffsetRequest"
	case *rawtopicreader.PartitionSessionStatusRequest:
		return "PartitionSessionStatusRequest"
	case *rawtopicreader.UpdateTokenRequest:
		return "UpdateTokenRequest"
	default:
		return fmt.Sprintf("Unknown(%T)", m)
	}
}

func (l *streamListener) receiveMessagesLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		mess, err := l.stream.Recv()

		logCtx := ctx
		if err != nil {
			if l.closing.Load() || ctx.Err() != nil {
				return
			}

			gtrace.TopicOnListenerReceiveMessage(l.tracer, &logCtx, l.listenerID, l.sessionID, "", 0, err)
			gtrace.TopicOnListenerError(l.tracer, &logCtx, l.listenerID, l.sessionID, err)
			l.goClose(ctx, xerrors.WithStackTrace(xerrors.Wrap(
				fmt.Errorf("ydb: failed read message from the stream in the topic reader listener: %w", err),
			)))

			return
		}

		messageType := reflect.TypeOf(mess).String()
		bytesSize := 0
		if mess, ok := mess.(*rawtopicreader.ReadResponse); ok {
			bytesSize = mess.BytesSize
		}

		gtrace.TopicOnListenerReceiveMessage(l.tracer, &logCtx, l.listenerID, l.sessionID, messageType, bytesSize, nil)

		if err := l.routeMessage(ctx, mess); err != nil {
			gtrace.TopicOnListenerError(l.tracer, &logCtx, l.listenerID, l.sessionID, err)
			l.goClose(ctx, err)
		}
	}
}

// routeMessage routes messages to appropriate handlers/workers
func (l *streamListener) routeMessage(ctx context.Context, mess rawtopicreader.ServerMessage) error {
	if l.closing.Load() {
		return nil
	}

	switch m := mess.(type) {
	case *rawtopicreader.StartPartitionSessionRequest:
		return l.handleStartPartition(ctx, m)
	case *rawtopicreader.StopPartitionSessionRequest:
		l.routeToWorker(m.PartitionSessionID, func(worker *PartitionWorker) {
			worker.AddRawServerMessage(m)
		})

		return nil
	case *rawtopicreader.ReadResponse:
		return l.splitAndRouteReadResponse(m)
	case *rawtopicreader.CommitOffsetResponse:
		return l.onCommitResponse(m)
	default:
		// Ignore unknown message types
		return nil
	}
}

// handleStartPartition creates a new worker and routes StartPartition message to it
func (l *streamListener) handleStartPartition(
	ctx context.Context,
	m *rawtopicreader.StartPartitionSessionRequest,
) error {
	session := topicreadercommon.NewPartitionSession(
		ctx,
		m.PartitionSession.Path,
		m.PartitionSession.PartitionID,
		l.cfg.readerID,
		l.sessionID,
		m.PartitionSession.PartitionSessionID,
		l.sessionIDCounter.Add(1),
		m.CommittedOffset,
	)
	if err := l.sessions.Add(session); err != nil {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("ydb: failed to add partition session: %w", err)))
	}

	// Create worker for this partition
	worker := l.createWorkerForPartition(session)

	// Send StartPartition message to the worker
	worker.AddRawServerMessage(m)

	return nil
}

// splitAndRouteReadResponse splits ReadResponse into batches and routes to workers
func (l *streamListener) splitAndRouteReadResponse(m *rawtopicreader.ReadResponse) error {
	batches, err := topicreadercommon.ReadRawBatchesToPublicBatches(m, l.sessions, l.cfg.Decoders)
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: failed to convert raw batches to public batches: %w", err)))
	}

	// Route each batch to its partition worker
	for _, batch := range batches {
		partitionSession := topicreadercommon.BatchGetPartitionSession(batch)
		routed := l.routeToWorker(partitionSession.StreamPartitionSessionID, func(worker *PartitionWorker) {
			worker.AddMessagesBatch(m.ServerMessageMetadata, batch)
		})
		if !routed {
			// Worker missing: batch is dropped but buffer was already charged above.
			// Return credit here; routeToWorker stays non-fatal for protocol mismatch.
			l.ReadBufferRelease(batchReadBufferSize(batch))
		}
	}

	return nil
}

// onCommitResponse processes CommitOffsetResponse directly in streamListener
// This prevents blocking commits in PartitionWorker threads
func (l *streamListener) onCommitResponse(msg *rawtopicreader.CommitOffsetResponse) error {
	for i := range msg.PartitionsCommittedOffsets {
		commit := &msg.PartitionsCommittedOffsets[i]

		var worker *PartitionWorker
		l.m.WithLock(func() {
			worker = l.workers[commit.PartitionSessionID]
		})

		if worker == nil {
			// Session not found - this can happen during shutdown, log but don't fail
			continue
		}

		session := worker.partitionSession

		// Update committed offset in the session
		session.SetCommittedOffsetForward(commit.CommittedOffset)

		// Notify the syncCommitter about the commit
		l.syncCommitter.OnCommitNotify(session, commit.CommittedOffset)

		// Emit trace event - use partition context instead of background
		logCtx := session.Context()
		gtrace.TopicOnReaderCommittedNotify(
			l.tracer,
			&logCtx,
			l.listenerID,
			session.Topic,
			session.PartitionID,
			session.StreamPartitionSessionID.ToInt64(),
			commit.CommittedOffset.ToInt64(),
		)
	}

	return nil
}

func (l *streamListener) sendCommit(b *topicreadercommon.PublicBatch) error {
	commitRanges := topicreadercommon.CommitRanges{
		Ranges: []topicreadercommon.CommitRange{topicreadercommon.GetCommitRange(b)},
	}

	if err := l.stream.Send(commitRanges.ToRawMessage()); err != nil {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("ydb: failed to send commit message: %w", err)))
	}

	return nil
}

// getSyncCommitter returns the syncCommitter for CommitHandler interface compatibility
func (l *streamListener) getSyncCommitter() SyncCommitter {
	return l.syncCommitter
}

// collectPendingFreeBytes drains all byte credits already queued in freeBytes after
// the first one was received by sendMessagesLoop.
func (l *streamListener) collectPendingFreeBytes(first int) int {
	sum := first
	for {
		select {
		case free := <-l.freeBytes:
			sum += free
		default:
			return sum
		}
	}
}

// ReadBufferRelease implements ReadBufferReleaser for partition workers.
func (l *streamListener) ReadBufferRelease(size int) {
	// Nothing was charged for this batch — avoid a pointless ReadRequest{BytesSize: 0}.
	if size == 0 {
		return
	}

	// May block when freeBytes is full — intentional backpressure on slow consumers.
	// On shutdown, skip send rather than block forever.
	select {
	case l.freeBytes <- size:
	case <-l.background.Context().Done():
	}
}

func (l *streamListener) sendMessage(m rawtopicreader.ClientMessage) {
	l.m.WithLock(func() {
		l.messagesToSend = append(l.messagesToSend, m)
	})

	select {
	case l.hasNewMessagesToSend <- empty.Struct{}:
	default:
	}
}

type confirmStorage[T any] struct {
	doneChan      empty.Chan
	confirmed     atomic.Bool
	val           T
	confirmAction sync.Once
	initAction    sync.Once
}

func (c *confirmStorage[T]) init() {
	c.initAction.Do(func() {
		c.doneChan = make(empty.Chan)
	})
}

func (c *confirmStorage[T]) Set(val T) {
	c.init()
	c.confirmAction.Do(func() {
		c.val = val
		c.confirmed.Store(true)
		close(c.doneChan)
	})
}

func (c *confirmStorage[T]) Done() empty.ChanReadonly {
	c.init()

	return c.doneChan
}

func (c *confirmStorage[T]) Get() (val T, ok bool) {
	c.init()

	if c.confirmed.Load() {
		return c.val, true
	}

	return val, false
}

// ReadSessionID returns the current read session identifier from the server.
func (l *streamListener) ReadSessionID() string {
	return l.sessionID
}

// SendRaw implements MessageSender for partition workers.
func (l *streamListener) SendRaw(msg rawtopicreader.ClientMessage) {
	l.sendMessage(msg)
}

// onWorkerStopped handles worker stopped notifications
func (l *streamListener) onWorkerStopped(
	worker *PartitionWorker,
	sessionID rawtopicreader.PartitionSessionID,
	reason error,
) {
	var (
		state       *partitionWorkerState
		closeWorker bool
	)
	l.m.WithLock(func() {
		// Do not remove a replacement worker that has already claimed this
		// session ID. Production workers pass their identity through the callback.
		if current := l.workers[sessionID]; current == worker {
			delete(l.workers, sessionID)
		}

		state = l.workerStateLocked(worker)
		closeWorker = !state.closeStarted
		state.closeStarted = true
	})

	if closeWorker {
		// onStopped runs from the worker callback itself. Closing synchronously
		// would wait for that callback and deadlock; the detached join also keeps
		// the retired-worker set bounded during normal operation.
		l.closeWorkerAfterStop(state, reason)
	}

	// Remove corresponding session
	for _, session := range l.sessions.GetAll() {
		if session.StreamPartitionSessionID == sessionID {
			_, _ = l.sessions.Remove(session.StreamPartitionSessionID)

			break
		}
	}

	// If reason from worker, propagate to streamListener shutdown
	// But avoid cascading shutdowns for normal lifecycle events like queue closure during shutdown
	if reason != nil && !l.closing.Load() {
		// Only propagate reason if we're not already closing
		// and if it's not a normal queue closure reason (which can happen during shutdown)
		if !xerrors.Is(reason, errPartitionQueueClosed) {
			l.goClose(l.background.Context(), reason)
		}
	}
}

// createWorkerForPartition creates a new PartitionWorker for the given session
func (l *streamListener) createWorkerForPartition(session *topicreadercommon.PartitionSession) *PartitionWorker {
	var worker *PartitionWorker
	worker = NewPartitionWorker(
		session.StreamPartitionSessionID,
		session,
		l,
		l.handler,
		func(sessionID rawtopicreader.PartitionSessionID, reason error) {
			l.onWorkerStopped(worker, sessionID, reason)
		},
		l.tracer,
		l.listenerID,
	)

	// Register ownership before starting the worker. Shutdown snapshots this
	// same state under the mutex after the stream loops have stopped.
	l.m.WithLock(func() {
		l.workers[session.StreamPartitionSessionID] = worker
		l.workerStateLocked(worker)
	})

	// Start worker
	worker.Start(l.background.Context())

	return worker
}

// routeToWorker routes a message to the appropriate worker.
// It returns false if the worker was not found (caller may need to free buffer credit).
func (l *streamListener) routeToWorker(
	partitionSessionID rawtopicreader.PartitionSessionID,
	routeFunc func(*PartitionWorker),
) bool {
	// Find worker by session
	var targetWorker *PartitionWorker
	l.m.WithLock(func() {
		targetWorker = l.workers[partitionSessionID]
	})

	if targetWorker != nil {
		routeFunc(targetWorker)

		return true
	}

	return false
}
