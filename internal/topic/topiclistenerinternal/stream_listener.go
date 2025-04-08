package topiclistenerinternal

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type streamListener struct {
	cfg *StreamListenerConfig

	stream      topicreadercommon.RawTopicReaderStream
	streamClose context.CancelCauseFunc
	handler     EventHandler
	sessionID   string

	background       background.Worker
	sessions         *topicreadercommon.PartitionSessionStorage
	sessionIDCounter *atomic.Int64

	hasNewMessagesToSend empty.Chan
	syncCommitter        *topicreadercommon.Committer

	closing atomic.Bool
	tracer  *trace.Topic

	m              xsync.Mutex
	messagesToSend []rawtopicreader.ClientMessage
}

func newStreamListener(
	connectionCtx context.Context,
	client TopicClient,
	eventListener EventHandler,
	config *StreamListenerConfig,
	sessionIDCounter *atomic.Int64,
) (*streamListener, error) {
	res := &streamListener{
		cfg:              config,
		handler:          eventListener,
		background:       *background.NewWorker(xcontext.ValueOnly(connectionCtx), "topic reader stream listener"),
		sessionIDCounter: sessionIDCounter,

		//nolint:godox
		tracer: &trace.Topic{}, // TODO: add read tracer
	}

	res.initVars(sessionIDCounter)
	if err := res.initStream(connectionCtx, client); err != nil {
		res.goClose(connectionCtx, err)

		return nil, err
	}

	res.syncCommitter = topicreadercommon.NewCommitterStopped(
		&trace.Topic{},
		res.background.Context(),
		topicreadercommon.CommitModeSync,
		res.stream.Send,
	)

	res.startBackground()
	res.sendDataRequest(config.BufferSize)

	return res, nil
}

func (l *streamListener) Close(ctx context.Context, reason error) error {
	if !l.closing.CompareAndSwap(false, true) {
		return errTopicListenerClosed
	}

	var resErrors []error

	// should be first because background wait stop of steams
	if l.stream != nil {
		l.streamClose(reason)
	}

	if err := l.background.Close(ctx, reason); err != nil {
		resErrors = append(resErrors, err)
	}

	if err := l.syncCommitter.Close(ctx, reason); err != nil {
		resErrors = append(resErrors, err)
	}

	for _, session := range l.sessions.GetAll() {
		session.Close()
		err := l.onStopPartitionRequest(session.Context(), &rawtopicreader.StopPartitionSessionRequest{
			ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
				Status: rawydb.StatusSuccess,
			},
			PartitionSessionID: session.StreamPartitionSessionID,
			Graceful:           false,
			CommittedOffset:    session.CommittedOffset(),
		})
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				resErrors = append(resErrors, err)
			}
		}
	}

	return errors.Join(resErrors...)
}

func (l *streamListener) goClose(ctx context.Context, reason error) {
	ctx, cancel := context.WithTimeout(xcontext.ValueOnly(ctx), time.Second)
	l.streamClose(reason)
	go func() {
		_ = l.background.Close(ctx, reason)
	}()

	cancel()
}

func (l *streamListener) startBackground() {
	l.background.Start("stream listener send loop", l.sendMessagesLoop)
	l.background.Start("stream listener receiver", l.receiveMessagesLoop)
	l.syncCommitter.Start()
}

func (l *streamListener) initVars(sessionIDCounter *atomic.Int64) {
	l.hasNewMessagesToSend = make(empty.Chan, 1)
	l.sessions = &topicreadercommon.PartitionSessionStorage{}
	l.sessionIDCounter = sessionIDCounter
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
			l.goClose(ctx, err)
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
		return xerrors.WithStackTrace(fmt.Errorf("ydb: failed to send init request for read stream in the listener: %w", err))
	}

	resp, err := l.stream.Recv()
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf(
			"ydb: failed to receive init response for read stream in the listener: %w",
			err,
		))
	}

	if status := resp.StatusData(); !status.Status.IsSuccess() {
		// wrap initialization error as operation status error - for handle with retrier
		// https://github.com/ydb-platform/ydb-go-sdk/issues/1361
		return xerrors.WithStackTrace(fmt.Errorf(
			"ydb: received bad status on init the topic stream listener: %v (%v)",
			status.Status,
			status.Issues,
		))
	}

	initResp, ok := resp.(*rawtopicreader.InitResponse)
	if !ok {
		return xerrors.WithStackTrace(fmt.Errorf(
			"bad message type on session init: %v (%v)",
			resp,
			reflect.TypeOf(resp),
		))
	}

	l.sessionID = initResp.SessionID

	return nil
}

func (l *streamListener) sendMessagesLoop(ctx context.Context) {
	chDone := ctx.Done()
	for {
		select {
		case <-chDone:
			return
		case <-l.hasNewMessagesToSend:
			var messages []rawtopicreader.ClientMessage
			l.m.WithLock(func() {
				messages = l.messagesToSend
				if len(messages) > 0 {
					l.messagesToSend = make([]rawtopicreader.ClientMessage, 0, cap(messages))
				}
			})

			for _, m := range messages {
				if err := l.stream.Send(m); err != nil {
					l.goClose(ctx, xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
						"ydb: failed send message by grpc to topic reader stream from listener: %w",
						err,
					))))

					return
				}
			}
		}
	}
}

func (l *streamListener) receiveMessagesLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		mess, err := l.stream.Recv()
		if err != nil {
			l.goClose(ctx, xerrors.WithStackTrace(
				fmt.Errorf("ydb: failed read message from the stream in the topic reader listener: %w", err),
			))

			return
		}

		l.onReceiveServerMessage(ctx, mess)
	}
}

func (l *streamListener) onReceiveServerMessage(ctx context.Context, mess rawtopicreader.ServerMessage) {
	var err error
	switch m := mess.(type) {
	case *rawtopicreader.StartPartitionSessionRequest:
		err = l.onStartPartitionRequest(ctx, m)
	case *rawtopicreader.StopPartitionSessionRequest:
		err = l.onStopPartitionRequest(ctx, m)
	case *rawtopicreader.ReadResponse:
		err = l.onReadResponse(m)
	case *rawtopicreader.CommitOffsetResponse:
		err = l.onCommitOffsetResponse(m)
	default:
		//nolint:godox
		// todo log
	}
	if err != nil {
		l.goClose(ctx, err)
	}
}

func (l *streamListener) onStartPartitionRequest(
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
		return err
	}

	resp := &rawtopicreader.StartPartitionSessionResponse{
		PartitionSessionID: m.PartitionSession.PartitionSessionID,
	}

	event := NewPublicStartPartitionSessionEvent(
		session.ToPublic(),
		m.CommittedOffset.ToInt64(),
		PublicOffsetsRange{
			Start: m.PartitionOffsets.Start.ToInt64(),
			End:   m.PartitionOffsets.End.ToInt64(),
		},
	)

	err := l.handler.OnStartPartitionSessionRequest(ctx, event)
	if err != nil {
		return err
	}

	var userResp PublicStartPartitionSessionConfirm
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-event.confirm.Done():
		userResp, _ = event.confirm.Get()
	}

	if userResp.readOffset != nil {
		resp.ReadOffset.Offset.FromInt64(*userResp.readOffset)
		resp.ReadOffset.HasValue = true
	}
	if userResp.CommitOffset != nil {
		resp.CommitOffset.Offset.FromInt64(*userResp.CommitOffset)
		resp.CommitOffset.HasValue = true
	}

	l.sendMessage(resp)

	return nil
}

func (l *streamListener) onStopPartitionRequest(
	ctx context.Context,
	m *rawtopicreader.StopPartitionSessionRequest,
) error {
	session, err := l.sessions.Get(m.PartitionSessionID)
	if err != nil {
		return err
	}

	handlerCtx := session.Context()

	event := NewPublicStopPartitionSessionEvent(
		session.ToPublic(),
		m.Graceful,
		m.CommittedOffset.ToInt64(),
	)

	if err = l.handler.OnStopPartitionSessionRequest(handlerCtx, event); err != nil {
		return err
	}

	go func() {
		// remove partition on the confirmation or on the listener closed
		select {
		case <-l.background.Done():
		case <-event.confirm.Done():
		}
		_, _ = l.sessions.Remove(m.PartitionSessionID)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-event.confirm.Done():
		// pass
	}

	if m.Graceful {
		l.sendMessage(&rawtopicreader.StopPartitionSessionResponse{PartitionSessionID: session.StreamPartitionSessionID})
	}

	return nil
}

func (l *streamListener) onReadResponse(m *rawtopicreader.ReadResponse) error {
	batches, err := topicreadercommon.ReadRawBatchesToPublicBatches(m, l.sessions, l.cfg.Decoders)
	if err != nil {
		return err
	}

	for _, batch := range batches {
		if err = l.handler.OnReadMessages(batch.Context(), NewPublicReadMessages(
			topicreadercommon.BatchGetPartitionSession(batch).ToPublic(),
			batch,
			l,
		)); err != nil {
			return err
		}
	}
	l.sendDataRequest(m.BytesSize)

	return nil
}

func (l *streamListener) sendCommit(b *topicreadercommon.PublicBatch) error {
	commitRanges := topicreadercommon.CommitRanges{
		Ranges: []topicreadercommon.CommitRange{topicreadercommon.GetCommitRange(b)},
	}

	return l.stream.Send(commitRanges.ToRawMessage())
}

func (l *streamListener) sendDataRequest(bytesCount int) {
	l.sendMessage(&rawtopicreader.ReadRequest{BytesSize: bytesCount})
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

func (l *streamListener) onCommitOffsetResponse(m *rawtopicreader.CommitOffsetResponse) error {
	for _, partOffset := range m.PartitionsCommittedOffsets {
		session, err := l.sessions.Get(partOffset.PartitionSessionID)
		if err != nil {
			return err
		}

		l.syncCommitter.OnCommitNotify(session, partOffset.CommittedOffset)
	}

	return nil
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
