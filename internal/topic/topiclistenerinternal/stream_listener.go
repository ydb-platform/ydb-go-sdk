package topiclistenerinternal

import (
	"context"
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"reflect"
	"time"
)

type streamListener struct {
	cfg StreamListenerConfig

	stream      rawtopicreader.TopicReaderStreamInterface
	streamClose context.CancelCauseFunc
	handler     EventHandler
	sessionID   string

	background background.Worker
	sessions   *topicreadercommon.PartitionSessionStorage

	hasNewMessagesToSend empty.Chan

	m              xsync.Mutex
	messagesToSend []rawtopicreader.ClientMessage
}

func newStreamListener(
	connectionCtx context.Context,
	client TopicClient,
	eventListener EventHandler,
	config StreamListenerConfig,
) (*streamListener, error) {
	res := &streamListener{
		cfg:        config,
		handler:    eventListener,
		background: *background.NewWorker(context.WithoutCancel(connectionCtx), "topic reader stream listener"),
	}
	res.initVars()

	if err := res.initStream(connectionCtx, client); err != nil {
		res.closeWithTimeout(connectionCtx, err)
		return nil, err
	}

	res.startBackground()
	res.sendDataRequest(config.BufferSize)
	return res, nil
}

func (l *streamListener) Close(ctx context.Context, reason error) error {
	var resErrors []error

	if l.stream != nil {
		l.streamClose(reason)
	}

	if err := l.background.Close(ctx, reason); err != nil {
		resErrors = append(resErrors, err)
	}

	for _, session := range l.sessions.GetAll() {
		session.Close()
		err := l.onStopPartitionRequest(session.Context(), &rawtopicreader.StopPartitionSessionRequest{
			ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
				Status: rawydb.StatusSuccess,
			},
			PartitionSessionID: session.PartitionSessionID,
			Graceful:           false,
			CommittedOffset:    session.CommittedOffset(),
		})
		if err != nil {
			resErrors = append(resErrors, err)
		}
	}

	return errors.Join(resErrors...)
}

func (l *streamListener) closeWithTimeout(ctx context.Context, reason error) {
	ctx, cancel := context.WithTimeout(xcontext.ValueOnly(ctx), time.Second)
	l.streamClose(reason)
	_ = l.background.Close(ctx, reason)

	cancel()
}

func (l *streamListener) startBackground() {
	l.background.Start("stream listener send loop", l.sendMessagesLoop)
	l.background.Start("stream listener receiver", l.receiveMessagesLoop)
}

func (l *streamListener) initVars() {
	l.hasNewMessagesToSend = make(empty.Chan, 1)
	l.sessions = &topicreadercommon.PartitionSessionStorage{}
}

func (l *streamListener) initStream(ctx context.Context, client TopicClient) error {
	streamCtx, streamClose := context.WithCancelCause(context.WithoutCancel(ctx))
	l.streamClose = streamClose
	initDone := make(empty.Chan)
	defer close(initDone)

	go func() {
		select {
		case <-ctx.Done():
			err := xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
				"ydb: topic listener stream init timeout: %w", ctx.Err(),
			)))
			l.closeWithTimeout(ctx, err)
			l.streamClose(err)
		case <-initDone:
			// pass
		}
	}()

	stream, err := client.StreamRead(streamCtx)
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: topic listener failed connect to a stream: %w",
			err,
		)))
	}
	l.stream = stream

	initMessage := topicreadercommon.CreateInitMessage(l.cfg.Consumer, l.cfg.Selectors)
	err = stream.Send(initMessage)
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: failed to send init request for read stream in the listener: %w", err))
	}

	resp, err := l.stream.Recv()
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: failed to receive init response for read stream in the listener: %w", err))
	}

	if status := resp.StatusData(); !status.Status.IsSuccess() {
		// TODO: better handler status error
		return xerrors.WithStackTrace(fmt.Errorf("ydb: received bad status on init the topic stream listener: %v (%v)", status.Status, status.Issues))
	}

	initResp, ok := resp.(*rawtopicreader.InitResponse)
	if !ok {
		return xerrors.WithStackTrace(fmt.Errorf("bad message type on session init: %v (%v)", resp, reflect.TypeOf(resp)))
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
					l.messagesToSend = make([]rawtopicreader.ClientMessage, 0, len(messages)*2)
				}
			})

			for _, m := range messages {
				if err := l.stream.Send(m); err != nil {
					l.closeWithTimeout(ctx, xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("ydb: failed send message by grpc to topic reader stream from listener: %w", err))))
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
			l.closeWithTimeout(ctx, xerrors.WithStackTrace(
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
		err = l.onReadResponse(ctx, m)
	}
	if err != nil {
		l.closeWithTimeout(ctx, err)
	}
}

func (l *streamListener) onStartPartitionRequest(ctx context.Context, m *rawtopicreader.StartPartitionSessionRequest) error {
	session := topicreadercommon.NewPartitionSession(
		ctx,
		m.PartitionSession.Path,
		m.PartitionSession.PartitionID,
		l.cfg.readerID,
		l.sessionID,
		m.PartitionSession.PartitionSessionID,
		m.CommittedOffset,
	)
	if err := l.sessions.Add(session); err != nil {
		return err
	}

	resp := &rawtopicreader.StartPartitionSessionResponse{
		PartitionSessionID: m.PartitionSession.PartitionSessionID,
	}

	handlerResp, err := l.handler.OnStartPartitionSessionRequest(ctx, PublicStartPartitionSessionRequest{
		PartitionSession: PublicPartitionSession{
			SessionID:   m.PartitionSession.PartitionSessionID.ToInt64(),
			TopicPath:   m.PartitionSession.Path,
			PartitionID: m.PartitionSession.PartitionID,
		},
		CommittedOffset: m.CommittedOffset.ToInt64(),
		PartitionOffsets: PublicOffsetsRange{
			Start: m.PartitionOffsets.Start.ToInt64(),
			End:   m.PartitionOffsets.End.ToInt64(),
		},
	})
	if supressUnimplemented(err) != nil {
		return err
	}
	if err == nil {
		if handlerResp.ReadOffset != nil {
			resp.ReadOffset.Offset = rawtopicreader.NewOffset(*handlerResp.ReadOffset)
			resp.ReadOffset.HasValue = true
		}
		if handlerResp.CommitOffset != nil {
			resp.CommitOffset.Offset = rawtopicreader.NewOffset(*handlerResp.CommitOffset)
			resp.CommitOffset.HasValue = true
		}
	}

	l.sendMessage(resp)
	return nil
}

func (l *streamListener) onStopPartitionRequest(ctx context.Context, m *rawtopicreader.StopPartitionSessionRequest) error {
	session, err := l.sessions.Remove(m.PartitionSessionID)
	if m.Graceful && err != nil {
		// stop partition may be received twice: graceful and force
		return err
	}

	var handlerCtx context.Context
	if session == nil {
		cancelledCtx, cancel := context.WithCancelCause(ctx)
		cancel(xerrors.WithStackTrace(xerrors.Wrap(errors.New(
			"ydb: partition on topic listener was force stopped: %w",
		))))
		handlerCtx = cancelledCtx
	} else {
		handlerCtx = session.Context()
	}

	if _, err := l.handler.OnStopPartitionSessionRequest(handlerCtx, PublicStopPartitionSessionRequest{
		PartitionSessionID: m.PartitionSessionID.ToInt64(),
		Graceful:           m.Graceful,
		CommittedOffset:    m.CommittedOffset.ToInt64(),
	}); supressUnimplemented(err) != nil {
		return err
	}

	if m.Graceful {
		l.sendMessage(&rawtopicreader.StopPartitionSessionResponse{PartitionSessionID: session.PartitionSessionID})
	}
	return nil
}

func (l *streamListener) onReadResponse(ctx context.Context, m *rawtopicreader.ReadResponse) error {
	batches, err := topicreadercommon.ReadRawBatchesToPublicBatches(m, l.sessions, l.cfg.Decoders)
	if err != nil {
		return err
	}

	for _, batch := range batches {
		if err = l.handler.OnReadMessages(batch.Context(), PublicReadMessages{
			PartitionSessionID: topicreadercommon.GetCommitRange(batch).PartitionSession.PartitionSessionID.ToInt64(),
			Batch:              batch,
		}); supressUnimplemented(err) != nil {
			return err
		}
	}
	l.sendDataRequest(m.BytesSize)
	return nil
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

func supressUnimplemented(err error) error {
	if errors.Is(err, ErrUnimplementedPublic) {
		return nil
	}
	return err
}
