package topiclistenerinternal

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"reflect"
)

type streamListener struct {
	stream     topicreaderinternal.RawTopicReaderStream
	handler    EventHandler
	bufferSize int64
	decoders   topicreadercommon.DecoderMap

	selectors []*topicreadercommon.PublicReadSelector
	consumer  string
	sessionID string

	background   background.Worker
	sendRequests chan rawtopicreader.ClientMessage
}

func newStreamListenerStopped(
	stream topicreaderinternal.RawTopicReaderStream,
	eventListener EventHandler,
	consumer string,
	selectors []*topicreadercommon.PublicReadSelector,
	bufferSize int64,
	decoders topicreadercommon.DecoderMap,
) *streamListener {
	res := &streamListener{
		stream:     stream,
		handler:    eventListener,
		consumer:   consumer,
		selectors:  selectors,
		bufferSize: bufferSize,
		decoders:   decoders,
	}
	res.initVars()
	return res
}

func (l *streamListener) Close(ctx context.Context, reason error) error {
	return l.background.Close(ctx, reason)
}

func (l *streamListener) start() error {
	if err := l.init(); err != nil {
		return err
	}

	l.background.Start("stream listener send loop", l.sendMessagesLoop)
	l.background.Start("stream listener receiver", l.receiveMessagesLoop)
	l.background.Start("stream listener request data loop", l.requestDataLoop)

	return nil

}

func (l *streamListener) initVars() {
	l.sendRequests = make(chan rawtopicreader.ClientMessage, 100)
}
func (l *streamListener) init() error {
	initMessage := topicreadercommon.CreateInitMessage(l.consumer, l.selectors)
	err := l.stream.Send(initMessage)
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
	panic("not implemented yet")
}

func (l *streamListener) receiveMessagesLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		mess, err := l.stream.Recv()
		if err != nil {
			_ = l.Close(ctx, xerrors.WithStackTrace(
				fmt.Errorf("ydb: failed read message from the stream in the topic reader listener: %w", err),
			))
			return
		}

		switch m := mess.(type) {
		case *rawtopicreader.ReadResponse:
			l.onReadResponse(ctx, m)
		}
	}
}

func (l *streamListener) onReadResponse(ctx context.Context, m *rawtopicreader.ReadResponse) {

}

func (l *streamListener) requestDataLoop(ctx context.Context) {
	panic("not implemented yet")
}
